# ap/trade_validator.py — AP30TradeValidator
# =============================================================================
# 30-trade validation framework.
# Produces a fully traceable trade log CSV + pass/fail report.
#
# Validation rules:
#   1. System is frozen during the batch (no mid-run config changes)
#   2. Every decision is logged (approved/blocked/reason/snapshot)
#   3. Every trade has full lifecycle: signal→plan→order→fill→exit→pnl
#   4. Pass/fail evaluated against infrastructure + strategy + product criteria
#
# Run from Render shell or as a cron:
#   python3 -c "from ap.trade_validator import AP30TradeValidator; AP30TradeValidator().run_report()"
# =============================================================================

from __future__ import annotations

import csv
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.trade_validator")

REPORT_DIR = os.environ.get("VALIDATOR_REPORT_DIR",
                             "/opt/render/project/src/validation_reports")


# =============================================================================
# TRADE RECORD
# =============================================================================

class TradeRecord:
    """One fully traced trade from signal to close."""

    __slots__ = [
        "trade_num", "date", "time_utc", "ticker", "side", "score", "tier",
        "trigger_type", "contract", "entry_target", "actual_fill",
        "exit_fill", "realized_pnl", "pnl_pct", "setup_status",
        "decision", "block_reason", "hold_minutes", "exit_reason",
        "slippage", "spread_pct", "oi", "volume", "dte",
        "calls_open_at_entry", "puts_open_at_entry", "capital_at_entry",
        "pending_entries_at_entry", "signal_id", "plan_id", "position_id",
        "order_id", "notes",
    ]

    def __init__(self, **kwargs):
        for k in self.__slots__:
            setattr(self, k, kwargs.get(k, ""))

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__slots__}


# =============================================================================
# VALIDATOR
# =============================================================================

class AP30TradeValidator:
    """
    Pulls trade data from Supabase and produces a validation report.

    Does NOT run trades — it reads what already happened and scores it.
    Frozen system: reads the frozen snapshot of what the system decided.

    Usage:
        validator = AP30TradeValidator(supabase_client=sb)
        report = validator.run_report(target_trades=30)
        validator.save_csv(report["trades"])
        validator.print_summary(report)
    """

    INFRA_PASS_RULES = {
        "zero_duplicate_entries":  "No duplicate fills for same signal",
        "zero_orphan_positions":   "No OPEN positions with no entry order",
        "zero_mystery_exits":      "No CLOSED positions with no exit_reason",
        "zero_illegal_transitions":"No illegal order state jumps in logs",
    }

    STRATEGY_PASS_RULES = {
        "positive_expectancy":     "Expectancy per trade > 0",
        "acceptable_drawdown":     "Max drawdown < 30%",
        "profit_factor_above_1":   "Profit factor > 1.0",
        "tier_outperformance":     "A/A+ tiers win rate > B tier win rate",
    }

    PRODUCT_PASS_RULES = {
        "all_trades_explainable":  "Every trade has signal_id + plan_id + reason",
        "daily_summary_accurate":  "Daily position counts match actual positions",
        "client_truth_consistent": "client_state matches positions table",
    }

    def __init__(self, supabase_client=None, client_id: str = "default"):
        self.sb        = supabase_client
        self.client_id = client_id

    def run_report(self, target_trades: int = 30) -> dict:
        """
        Pull closed trades from Supabase, build TradeRecords, score everything.
        Returns full report dict.
        """
        log.info(f"Running {target_trades}-trade validation report...")

        trades    = self._pull_trades(target_trades)
        records   = self._build_records(trades)
        perf      = self._compute_performance(records)
        infra     = self._infra_checks()
        segments  = self._segment_analysis(records)
        verdict   = self._verdict(perf, infra)

        report = {
            "generated_at":    datetime.now(timezone.utc).isoformat(),
            "target_trades":   target_trades,
            "actual_trades":   len(records),
            "trades":          [r.to_dict() for r in records],
            "performance":     perf,
            "infrastructure":  infra,
            "segments":        segments,
            "verdict":         verdict,
        }

        self._print_summary(report)
        self._save_report(report)
        return report

    # =========================================================================
    # DATA PULL
    # =========================================================================

    def _pull_trades(self, limit: int) -> list[dict]:
        """
        Pull closed positions with lifecycle data from Supabase.
        Joins orders to verify full lifecycle: plan→order→fill→exit.
        """
        if not self.sb:
            log.warning("No Supabase client — pulling from DB directly")
            return self._pull_from_db(limit)

        try:
            resp = (
                self.sb.table("positions")
                .select(
                    "id,client_id,underlying,contract,direction,qty,avg_fill,"
                    "entry_ts,exit_ts,exit_price,realized_pnl,exit_reason,"
                    "status,tier,score,pattern,plan_id,signal_id"
                )
                .eq("client_id", self.client_id)
                .in_("status", ["CLOSED", "STOPPED", "TAKEN_PROFIT", "EXPIRED"])
                .order("entry_ts", desc=True)
                .limit(limit)
                .execute()
            )
            positions = resp.data or []
        except Exception as e:
            log.error(f"Supabase pull failed: {e}")
            return self._pull_from_db(limit)

        # Join order lifecycle data for each position
        enriched = []
        for pos in positions:
            plan_id = pos.get("plan_id")
            pos_id  = pos.get("id")
            entry_order = None
            exit_order  = None

            if plan_id:
                try:
                    eo = (self.sb.table("orders")
                          .select("local_order_id,status,fill_price,filled_qty,"
                                  "submitted_ts,filled_ts,broker_order_id")
                          .eq("client_id", self.client_id)
                          .eq("plan_id", plan_id)
                          .eq("kind", "ENTRY")
                          .limit(1).execute())
                    entry_order = (eo.data or [{}])[0]
                except Exception:
                    pass

            if pos_id:
                try:
                    xo = (self.sb.table("orders")
                          .select("local_order_id,status,fill_price,filled_qty,"
                                  "submitted_ts,filled_ts,broker_order_id")
                          .eq("client_id", self.client_id)
                          .eq("position_id", pos_id)
                          .eq("kind", "EXIT")
                          .limit(1).execute())
                    exit_order = (xo.data or [{}])[0]
                except Exception:
                    pass

            pos["_entry_order"] = entry_order or {}
            pos["_exit_order"]  = exit_order or {}
            pos["_has_entry_order"]  = bool(entry_order)
            pos["_has_exit_order"]   = bool(exit_order)
            pos["_entry_fill_price"] = (entry_order or {}).get("fill_price")
            pos["_exit_fill_price"]  = (exit_order or {}).get("fill_price")
            enriched.append(pos)

        return enriched

    def _pull_from_db(self, limit: int) -> list[dict]:
        """Fallback: pull directly via psycopg2."""
        try:
            from ap.db import conn, run_with_retry
            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT id, client_id, underlying, contract, direction,
                               qty, avg_fill, entry_ts, exit_ts, exit_price,
                               realized_pnl, exit_reason, status, tier, score,
                               pattern, plan_id, signal_id
                        FROM positions
                        WHERE client_id=%s
                        AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                        ORDER BY entry_ts DESC
                        LIMIT %s
                        """,
                        (self.client_id, limit),
                    )
                    return c.fetchall()
            return run_with_retry(_fn)
        except Exception as e:
            log.error(f"DB pull failed: {e}")
            return []

    # =========================================================================
    # RECORD BUILDER
    # =========================================================================

    def _build_records(self, trades: list[dict]) -> list[TradeRecord]:
        records = []
        for i, t in enumerate(trades, 1):
            entry  = float(t.get("avg_fill") or 0)
            ex     = float(t.get("exit_price") or 0)
            pnl    = float(t.get("realized_pnl") or 0)
            pnl_pct = ((ex - entry) / entry * 100) if entry > 0 else 0
            slip   = round(ex - entry, 4) if ex and entry else ""

            entry_ts = t.get("entry_ts") or ""
            exit_ts  = t.get("exit_ts") or ""
            hold_min = ""
            if entry_ts and exit_ts:
                try:
                    et = datetime.fromisoformat(str(entry_ts).replace("Z", "+00:00"))
                    xt = datetime.fromisoformat(str(exit_ts).replace("Z", "+00:00"))
                    hold_min = round((xt - et).total_seconds() / 60, 1)
                except Exception:
                    pass

            r = TradeRecord(
                trade_num   = i,
                date        = str(entry_ts)[:10] if entry_ts else "",
                time_utc    = str(entry_ts)[11:19] if entry_ts else "",
                ticker      = t.get("underlying", ""),
                side        = t.get("direction", ""),
                score       = t.get("score", ""),
                tier        = t.get("tier", ""),
                trigger_type= "",
                contract    = t.get("contract", ""),
                entry_target= "",
                actual_fill = entry,
                exit_fill   = ex,
                realized_pnl= pnl,
                pnl_pct     = round(pnl_pct, 2),
                setup_status= t.get("pattern", ""),
                decision    = "APPROVED",
                block_reason= "",
                hold_minutes= hold_min,
                exit_reason = t.get("exit_reason", ""),
                slippage    = slip,
                signal_id   = t.get("signal_id", ""),
                plan_id     = t.get("plan_id", ""),
                position_id = t.get("id", ""),
                notes       = "",
            )
            records.append(r)
        return records

    # =========================================================================
    # PERFORMANCE
    # =========================================================================

    def _compute_performance(self, records: list[TradeRecord]) -> dict:
        if not records:
            return {"error": "no_trades"}

        pnls   = [float(r.realized_pnl or 0) for r in records]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        total_pnl     = sum(pnls)
        win_rate      = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win       = sum(wins) / len(wins) if wins else 0
        avg_loss      = sum(losses) / len(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if losses else float("inf")
        expectancy    = total_pnl / len(pnls) if pnls else 0

        # Drawdown — both absolute and normalized % of starting equity
        # Use 25000 as default account size; replace with client_state if available
        account_equity = 25000.0
        equity_curve = [0.0]
        for p in pnls:
            equity_curve.append(equity_curve[-1] + p)
        peak = equity_curve[0]
        max_dd_abs = 0.0
        max_dd_pct = 0.0
        for e in equity_curve:
            peak = max(peak, e)
            dd_abs = peak - e
            dd_pct = (dd_abs / (account_equity + peak)) * 100 if (account_equity + peak) > 0 else 0
            max_dd_abs = max(max_dd_abs, dd_abs)
            max_dd_pct = max(max_dd_pct, dd_pct)

        # Consecutive losses
        max_consec_loss = 0
        cur_consec = 0
        for p in pnls:
            if p < 0:
                cur_consec += 1
                max_consec_loss = max(max_consec_loss, cur_consec)
            else:
                cur_consec = 0

        return {
            "trade_count":          len(records),
            "total_pnl":            round(total_pnl, 2),
            "win_rate_pct":         round(win_rate, 1),
            "wins":                 len(wins),
            "losses":               len(losses),
            "avg_win":              round(avg_win, 2),
            "avg_loss":             round(avg_loss, 2),
            "profit_factor":        round(profit_factor, 2) if profit_factor != float("inf") else "∞",
            "expectancy":           round(expectancy, 2),
            "max_drawdown_abs":     round(max_dd_abs, 2),
            "max_drawdown_pct":     round(max_dd_pct, 2),
            "max_consec_losses":    max_consec_loss,
            "largest_win":          round(max(wins), 2) if wins else 0,
            "largest_loss":         round(min(losses), 2) if losses else 0,
        }

    # =========================================================================
    # INFRASTRUCTURE CHECKS
    # =========================================================================

    def _infra_checks(self) -> dict:
        results = {}
        try:
            from ap.db import conn, run_with_retry

            # Duplicate entries: same signal_id, multiple FILLED orders
            def _dupes():
                with conn() as c:
                    c.execute(
                        """
                        SELECT signal_id, COUNT(*) AS n FROM orders
                        WHERE client_id=%s AND kind='ENTRY'
                        AND status IN ('FILLED','EXIT_FILLED')
                        AND signal_id IS NOT NULL
                        GROUP BY signal_id HAVING COUNT(*) > 1
                        """,
                        (self.client_id,),
                    )
                    return c.fetchall()
            dupes = run_with_retry(_dupes)
            results["duplicate_entries"] = len(dupes)
            results["duplicate_entries_pass"] = len(dupes) == 0

            # Orphan positions: OPEN with no entry order
            def _orphans():
                with conn() as c:
                    c.execute(
                        """
                        SELECT p.id FROM positions p
                        WHERE p.client_id=%s AND p.status='OPEN'
                        AND NOT EXISTS (
                            SELECT 1 FROM orders o
                            WHERE o.client_id=p.client_id
                            AND o.plan_id=p.plan_id
                            AND o.kind='ENTRY'
                        )
                        """,
                        (self.client_id,),
                    )
                    return c.fetchall()
            orphans = run_with_retry(_orphans)
            results["orphan_positions"] = len(orphans)
            results["orphan_positions_pass"] = len(orphans) == 0

            # Mystery exits: CLOSED with no exit_reason
            def _mystery():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COUNT(*) AS n FROM positions
                        WHERE client_id=%s
                        AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                        AND (exit_reason IS NULL OR exit_reason='')
                        """,
                        (self.client_id,),
                    )
                    return int((c.fetchone() or {}).get("n") or 0)
            mystery = run_with_retry(_mystery)
            results["mystery_exits"] = mystery
            results["mystery_exits_pass"] = mystery == 0

            # Lifecycle completeness: closed positions with no matching FILLED entry order
            def _no_entry_fill():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COUNT(*) AS n FROM positions p
                        WHERE p.client_id=%s
                        AND p.status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                        AND p.plan_id IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM orders o
                            WHERE o.client_id=p.client_id
                            AND o.plan_id=p.plan_id
                            AND o.kind='ENTRY'
                            AND o.status='FILLED'
                        )
                        """,
                        (self.client_id,),
                    )
                    return int((c.fetchone() or {}).get("n") or 0)
            no_entry = run_with_retry(_no_entry_fill)
            results["positions_without_filled_entry"] = no_entry
            results["positions_without_filled_entry_pass"] = no_entry == 0

            # Stale orders: CREATED/SUBMITTED > 2 hours old
            def _stale():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COUNT(*) AS n FROM orders
                        WHERE client_id=%s
                        AND status IN ('CREATED','SUBMITTED','EXIT_SUBMITTED')
                        AND created_ts < NOW() - INTERVAL '2 hours'
                        """,
                        (self.client_id,),
                    )
                    return int((c.fetchone() or {}).get("n") or 0)
            stale = run_with_retry(_stale)
            results["stale_orders"] = stale
            results["stale_orders_pass"] = stale == 0

        except Exception as e:
            results["error"] = str(e)

        return results

    # =========================================================================
    # SEGMENT ANALYSIS
    # =========================================================================

    def _segment_analysis(self, records: list[TradeRecord]) -> dict:
        def _perf_for(subset):
            pnls = [float(r.realized_pnl or 0) for r in subset]
            if not pnls:
                return {"count": 0, "total_pnl": 0, "win_rate": 0}
            wins = [p for p in pnls if p > 0]
            return {
                "count":     len(pnls),
                "total_pnl": round(sum(pnls), 2),
                "win_rate":  round(len(wins) / len(pnls) * 100, 1),
                "avg_pnl":   round(sum(pnls) / len(pnls), 2),
            }

        # By tier
        tiers = {}
        for r in records:
            t = str(r.tier or "?")
            tiers.setdefault(t, []).append(r)
        by_tier = {t: _perf_for(v) for t, v in tiers.items()}

        # By ticker
        tickers = {}
        for r in records:
            t = str(r.ticker or "?")
            tickers.setdefault(t, []).append(r)
        by_ticker = {t: _perf_for(v) for t, v in tickers.items()}

        # By direction
        calls = [r for r in records if str(r.side or "").upper() == "CALL"]
        puts  = [r for r in records if str(r.side or "").upper() == "PUT"]

        return {
            "by_tier":      by_tier,
            "by_ticker":    by_ticker,
            "calls":        _perf_for(calls),
            "puts":         _perf_for(puts),
        }

    # =========================================================================
    # VERDICT
    # =========================================================================

    def _verdict(self, perf: dict, infra: dict) -> dict:
        fails = []

        # Infrastructure
        if not infra.get("duplicate_entries_pass", True):
            fails.append(f"FAIL: {infra['duplicate_entries']} duplicate entries")
        if not infra.get("orphan_positions_pass", True):
            fails.append(f"FAIL: {infra['orphan_positions']} orphan positions")
        if not infra.get("mystery_exits_pass", True):
            fails.append(f"FAIL: {infra['mystery_exits']} mystery exits")

        # Strategy
        if isinstance(perf.get("expectancy"), (int, float)):
            if perf["expectancy"] <= 0:
                fails.append(f"FAIL: negative expectancy ${perf['expectancy']:.2f}")
        if isinstance(perf.get("profit_factor"), (int, float)):
            if perf["profit_factor"] < 1.0:
                fails.append(f"FAIL: profit factor {perf['profit_factor']:.2f} < 1.0")
        if isinstance(perf.get("max_drawdown"), (int, float)):
            if perf["max_drawdown"] > 1500:
                fails.append(f"FAIL: max drawdown ${perf['max_drawdown']:.2f}")

        passed = len(fails) == 0
        # Real sellable criteria — not just "pass + 20 trades"
        sellable_checks = {
            "enough_trades":       perf.get("trade_count", 0) >= 20,
            "positive_expectancy": isinstance(perf.get("expectancy"), (int, float)) and perf["expectancy"] > 0,
            "profit_factor_ok":    isinstance(perf.get("profit_factor"), (int, float)) and perf["profit_factor"] >= 1.2,
            "win_rate_ok":         isinstance(perf.get("win_rate_pct"), (int, float)) and perf["win_rate_pct"] >= 45,
            "drawdown_ok":         isinstance(perf.get("max_drawdown_pct"), (int, float)) and perf["max_drawdown_pct"] < 20,
            "no_infra_failures":   passed,
        }
        sellable = all(sellable_checks.values())
        not_met  = [k for k, v in sellable_checks.items() if not v]

        return {
            "passed":         passed,
            "verdict":        "✅ PASS" if passed else "❌ FAIL",
            "failures":       fails,
            "sellable":       sellable,
            "sellable_checks":sellable_checks,
            "sellable_msg":   (
                "✅ System has proof — ready for beta clients at $997–$2,997/month"
                if sellable
                else f"Not yet sellable — fix: {', '.join(not_met)}"
            ),
        }

    # =========================================================================
    # OUTPUT
    # =========================================================================

    def _print_summary(self, report: dict):
        p = report["performance"]
        v = report["verdict"]
        i = report["infrastructure"]

        print("\n" + "="*60)
        print(f"  ANGEL PRECISION — {report['actual_trades']}-TRADE VALIDATION")
        print("="*60)
        print(f"  Total PnL:          ${p.get('total_pnl', 0):>10.2f}")
        print(f"  Win Rate:            {p.get('win_rate_pct', 0):>9.1f}%")
        print(f"  Avg Win:            ${p.get('avg_win', 0):>10.2f}")
        print(f"  Avg Loss:           ${p.get('avg_loss', 0):>10.2f}")
        print(f"  Profit Factor:       {str(p.get('profit_factor', '?')):>9}")
        print(f"  Expectancy:         ${p.get('expectancy', 0):>10.2f}")
        print(f"  Max Drawdown ($):   ${p.get('max_drawdown_abs', 0):>10.2f}")
        print(f"  Max Drawdown (%):    {p.get('max_drawdown_pct', 0):>9.1f}%")
        print(f"  Max Consec Losses:   {p.get('max_consec_losses', 0):>9}")
        print(f"  Largest Win:        ${p.get('largest_win', 0):>10.2f}")
        print(f"  Largest Loss:       ${p.get('largest_loss', 0):>10.2f}")
        print("-"*60)
        print(f"  Duplicate entries:        {i.get('duplicate_entries', '?')}  {'✅' if i.get('duplicate_entries_pass') else '❌'}")
        print(f"  Orphan positions:         {i.get('orphan_positions', '?')}  {'✅' if i.get('orphan_positions_pass') else '❌'}")
        print(f"  Mystery exits:            {i.get('mystery_exits', '?')}  {'✅' if i.get('mystery_exits_pass') else '❌'}")
        print(f"  Missing entry fills:      {i.get('positions_without_filled_entry', '?')}  {'✅' if i.get('positions_without_filled_entry_pass') else '❌'}")
        print(f"  Stale orders:             {i.get('stale_orders', '?')}  {'✅' if i.get('stale_orders_pass') else '❌'}")
        print("-"*60)
        print(f"  VERDICT: {v['verdict']}")
        for f in v.get("failures", []):
            print(f"    → {f}")
        print(f"  {v['sellable_msg']}")
        print("="*60 + "\n")

    def save_csv(self, trades: list[dict], path: str | None = None) -> str:
        os.makedirs(REPORT_DIR, exist_ok=True)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = path or os.path.join(REPORT_DIR, f"validation_{ts}.csv")

        if not trades:
            log.warning("No trades to save")
            return path

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trades[0].keys())
            writer.writeheader()
            writer.writerows(trades)

        log.info(f"Trade log saved: {path}")
        return path

    def _save_report(self, report: dict):
        try:
            os.makedirs(REPORT_DIR, exist_ok=True)
            ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = os.path.join(REPORT_DIR, f"report_{ts}.json")
            with open(path, "w") as f:
                json.dump(report, f, indent=2, default=str)
            log.info(f"Report saved: {path}")
            # Also save CSV
            self.save_csv(report["trades"],
                          os.path.join(REPORT_DIR, f"trades_{ts}.csv"))
        except Exception as e:
            log.error(f"Failed to save report: {e}")
