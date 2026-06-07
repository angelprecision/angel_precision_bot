# =============================================================================
# ap_operator_weekly_rollup.py  —  PR #91 Weekly Archive from Daily Artifacts
# =============================================================================
# The weekly archive does NOT rebuild itself from live queries. It reads
# operator_daily_folders (the durable Supabase artifact from PR91 daily
# rollup) and aggregates only what's there.
#
# Rules from PR91 spec:
#   * Missing days appear in days_missing — never silently skipped.
#   * Partial days surface their section_errors / missing_sections per day.
#   * Official live totals only count PR90 Tradier Exit Proof Lock
#     eligible rows.
#   * No trading logic touched.
#
# Public API:
#   build_weekly_rollup(iso_week, *, conn_factory=None,
#                       disk_export=False, disk_root=None)
#   upsert_weekly_rollup(rollup, *, conn_factory=None)
#   fetch_weekly_rollup(iso_week, *, conn_factory=None)
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from ap_operator_daily_folders import (
    SECTION_NAMES,
    iso_week_bounds,
    iso_week_string,
    write_disk_export,
    DailyFolder,
)

log = logging.getLogger("ap.operator_weekly_rollup")


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _default_conn_factory() -> Optional[Callable[[], Any]]:
    try:
        from ap.db import conn  # type: ignore
        return conn
    except Exception:
        return None


def _fetch_week_rows(
    iso_week: str,
    *,
    conn_factory: Optional[Callable[[], Any]],
) -> list[dict[str, Any]]:
    """Pull every operator_daily_folders row whose iso_week matches.
    Returns [] if Supabase unavailable; caller handles."""
    if conn_factory is None:
        return []
    try:
        with conn_factory() as cur:
            cur.execute(
                "SELECT * FROM operator_daily_folders WHERE iso_week=%s ORDER BY folder_date ASC",
                [iso_week],
            )
            desc = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                if isinstance(r, dict):
                    out.append(dict(r))
                else:
                    out.append(dict(zip(desc, r)))
            return out
    except Exception as e:
        log.warning("weekly rollup: read daily folders failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

# Keys we sum across daily summaries. Each daily summary value is either
# an int (countable) or None (unavailable). We sum only ints, and treat
# None as "skip" (NOT as 0).
_SUM_INT_KEYS = (
    "total_signals", "admitted_signals", "rejected_signals",
    "orders_created", "broker_submitted", "orders_filled",
    "orders_failed",
    "official_live_trades", "unofficial_unreconciled_trades",
    "missing_exit_fill_truth",
    "closed_trades", "wins", "losses",
    "client_discrepancy_count",
)


def _coerce_summary(row: dict[str, Any]) -> dict[str, Any]:
    """summary column is JSONB; psycopg2 may return dict or str."""
    s = row.get("summary")
    if isinstance(s, str):
        try:
            return json.loads(s)
        except Exception:
            return {}
    if isinstance(s, dict):
        return s
    return {}


def _coerce_jsonb(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    if value is None:
        return default
    return value


def _aggregate_totals(daily_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Roll daily summaries into weekly totals.

    NONE means at least one day's value was unavailable; we surface that
    as a partial total by leaving it None rather than faking a zero.
    The PR91 spec is explicit: do NOT show fake zeros for missing data.
    """
    totals: dict[str, Any] = {k: 0 for k in _SUM_INT_KEYS}
    any_unavailable: dict[str, bool] = {k: False for k in _SUM_INT_KEYS}

    # Top stages/reasons aggregated across days
    top_drop_buckets: dict[str, int] = {}
    top_failure_stage_buckets: dict[str, int] = {}
    top_failure_reason_buckets: dict[str, int] = {}

    win_rate_samples: list[float] = []

    for row in daily_rows:
        summary = _coerce_summary(row)
        for k in _SUM_INT_KEYS:
            v = summary.get(k)
            if v is None:
                any_unavailable[k] = True
                continue
            try:
                totals[k] += int(v)
            except (TypeError, ValueError):
                any_unavailable[k] = True

        for bucket, key in (
            (top_drop_buckets,           "top_drop_stage"),
            (top_failure_stage_buckets,  "top_failure_stage"),
            (top_failure_reason_buckets, "top_failure_reason"),
        ):
            v = summary.get(key)
            if v:
                bucket[v] = bucket.get(v, 0) + 1

        wr = summary.get("win_rate")
        try:
            if wr is not None:
                win_rate_samples.append(float(wr))
        except (TypeError, ValueError):
            pass

    # Convert "all-None" sums back to None per the No-Fake-Zeros rule.
    # Sums where at least one day was unavailable get a "partial" flag.
    out: dict[str, Any] = {}
    partial_keys: list[str] = []
    for k in _SUM_INT_KEYS:
        if any_unavailable[k] and totals[k] == 0:
            out[k] = None
        else:
            out[k] = totals[k]
            if any_unavailable[k]:
                partial_keys.append(k)

    out["partial_keys"] = partial_keys

    # Win rate: average across days that reported one. None if zero samples.
    if win_rate_samples:
        out["win_rate"] = round(sum(win_rate_samples) / len(win_rate_samples), 2)
    else:
        out["win_rate"] = None

    out["top_drop_stage"]     = _pick_top(top_drop_buckets)
    out["top_failure_stage"]  = _pick_top(top_failure_stage_buckets)
    out["top_failure_reason"] = _pick_top(top_failure_reason_buckets)
    return out


def _pick_top(bucket: dict[str, int]) -> Optional[str]:
    if not bucket:
        return None
    return max(bucket.items(), key=lambda kv: kv[1])[0]


# ---------------------------------------------------------------------------
# Weekly rollup dataclass
# ---------------------------------------------------------------------------

@dataclass
class WeeklyRollup:
    iso_week:                str
    week_start:              _date
    week_end:                _date
    generated_at:            str
    days_present:            list[str]                     = field(default_factory=list)
    days_missing:            list[str]                     = field(default_factory=list)
    daily_index:             list[dict[str, Any]]          = field(default_factory=list)
    section_errors_by_day:   dict[str, dict[str, str]]     = field(default_factory=dict)
    missing_sections_by_day: dict[str, list[str]]          = field(default_factory=dict)
    source_status_by_day:    dict[str, str]                = field(default_factory=dict)
    totals:                  dict[str, Any]                = field(default_factory=dict)
    # PR91 amendment: backward-compat warnings (legacy rows missing iso_week,
    # legacy schema differences, etc.). Surfaced in to_public() so the
    # weekly dashboard can flag the day rather than crashing.
    compatibility_warnings:  list[dict[str, Any]]          = field(default_factory=list)

    def to_db_row(self) -> dict[str, Any]:
        # NOTE: 'id' (BIGSERIAL on fresh installs) is never set by us.
        # We ONLY write the columns the PR91 migration guarantees exist
        # on every install (legacy or fresh). The ON CONFLICT target is
        # the uq_weekly_rollups_iso_week unique partial index.
        return {
            "iso_week":                self.iso_week,
            "week_start":              self.week_start.isoformat(),
            "week_end":                self.week_end.isoformat(),
            "generated_at":            self.generated_at,
            "days_present":            list(self.days_present),
            "days_missing":            list(self.days_missing),
            "daily_index":             list(self.daily_index),
            "section_errors_by_day":   dict(self.section_errors_by_day),
            "missing_sections_by_day": dict(self.missing_sections_by_day),
            "source_status_by_day":    dict(self.source_status_by_day),
            "totals":                  dict(self.totals),
        }

    def to_public(self) -> dict[str, Any]:
        """Return the spec-pinned public weekly shape with all the fields
        listed in PR91 §6 surfaced at the top level."""
        t = self.totals or {}
        return {
            "iso_week":                self.iso_week,
            "week_start":              self.week_start.isoformat(),
            "week_end":                self.week_end.isoformat(),
            "generated_at":            self.generated_at,
            "days_present":            list(self.days_present),
            "days_missing":            list(self.days_missing),
            "daily_index":             list(self.daily_index),
            "section_errors_by_day":   dict(self.section_errors_by_day),
            "missing_sections_by_day": dict(self.missing_sections_by_day),
            "source_status_by_day":    dict(self.source_status_by_day),
            # Spec §6 top-level totals
            "total_signals":                  t.get("total_signals"),
            "admitted_signals":               t.get("admitted_signals"),
            "rejected_signals":               t.get("rejected_signals"),
            "orders_created":                 t.get("orders_created"),
            "broker_submitted":               t.get("broker_submitted"),
            "orders_filled":                  t.get("orders_filled"),
            "orders_failed":                  t.get("orders_failed"),
            "official_live_trades":           t.get("official_live_trades"),
            "unofficial_unreconciled_trades": t.get("unofficial_unreconciled_trades"),
            "missing_exit_fill_truth":        t.get("missing_exit_fill_truth"),
            "closed_trades":                  t.get("closed_trades"),
            "wins":                           t.get("wins"),
            "losses":                         t.get("losses"),
            "win_rate":                       t.get("win_rate"),
            "top_drop_stage":                 t.get("top_drop_stage"),
            "top_failure_stage":              t.get("top_failure_stage"),
            "top_failure_reason":             t.get("top_failure_reason"),
            "client_discrepancy_count":       t.get("client_discrepancy_count"),
            "partial_keys":                   t.get("partial_keys", []),
            # PR91 amendment: backward-compat warnings (always present,
            # empty list when the schema is fully PR91-shaped).
            "compatibility_warnings":         list(self.compatibility_warnings),
        }


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def build_weekly_rollup(
    iso_week_or_date: Any,
    *,
    conn_factory=None,
    disk_export: bool = False,
    disk_root: Optional[str] = None,
) -> WeeklyRollup:
    """Aggregate operator_daily_folders rows for the given ISO week (or
    the ISO week of the given date).

    The weekly rollup is BUILT FROM DAILY ARTIFACTS, never from live
    queries. If a daily row is missing for any weekday in the week, that
    date is listed in days_missing.
    """
    if isinstance(iso_week_or_date, str) and "-W" in iso_week_or_date:
        iso = iso_week_or_date
    else:
        iso = iso_week_string(iso_week_or_date)

    week_start, week_end = iso_week_bounds(iso)
    generated_at = datetime.now(timezone.utc).isoformat()

    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    rows = _fetch_week_rows(iso, conn_factory=cf)

    # Map by date string for lookup
    by_date: dict[str, dict[str, Any]] = {}
    for r in rows:
        fd = r.get("folder_date")
        if isinstance(fd, (datetime, _date)):
            key = fd.isoformat()[:10]
        elif isinstance(fd, str):
            key = fd[:10]
        else:
            continue
        by_date[key] = r

    # We iterate every calendar day Mon..Sun in the ISO week. Trading
    # days are Mon-Fri; weekends are still listed but generally show as
    # missing — that is honest, NOT a fake zero.
    days_present: list[str] = []
    days_missing: list[str] = []
    daily_index: list[dict[str, Any]] = []
    section_errors_by_day: dict[str, dict[str, str]] = {}
    missing_sections_by_day: dict[str, list[str]] = {}
    source_status_by_day: dict[str, str] = {}

    cur_day = week_start
    while cur_day <= week_end:
        key = cur_day.isoformat()
        row = by_date.get(key)
        if row is None:
            days_missing.append(key)
            source_status_by_day[key] = "missing"
            daily_index.append({
                "date":           key,
                "status":         "missing",
                "iso_week":       iso,
                "section_errors": {},
                "missing_sections": [],
                "generated_at":   None,
            })
        else:
            days_present.append(key)
            sec_err  = _coerce_jsonb(row.get("section_errors"), {})
            missing  = _coerce_jsonb(row.get("missing_sections"), [])
            ss       = row.get("source_status") or "ok"

            section_errors_by_day[key]   = dict(sec_err) if isinstance(sec_err, dict) else {}
            missing_sections_by_day[key] = list(missing) if isinstance(missing, list) else []
            source_status_by_day[key]    = ss

            status_label = "complete" if (ss == "ok" and not missing) else "partial"
            daily_index.append({
                "date":             key,
                "status":           status_label,
                "iso_week":         iso,
                "section_errors":   section_errors_by_day[key],
                "missing_sections": missing_sections_by_day[key],
                "generated_at":     row.get("generated_at"),
            })
        cur_day += timedelta(days=1)

    totals = _aggregate_totals(rows)

    rollup = WeeklyRollup(
        iso_week=iso,
        week_start=week_start,
        week_end=week_end,
        generated_at=generated_at,
        days_present=days_present,
        days_missing=days_missing,
        daily_index=daily_index,
        section_errors_by_day=section_errors_by_day,
        missing_sections_by_day=missing_sections_by_day,
        source_status_by_day=source_status_by_day,
        totals=totals,
    )

    if disk_export:
        try:
            write_weekly_disk_export(rollup, daily_rows=rows,
                                     disk_root=disk_root or _default_disk_root())
        except Exception as e:
            log.warning("weekly disk export failed for %s: %s", iso, e)

    return rollup


# ---------------------------------------------------------------------------
# Upsert / fetch
# ---------------------------------------------------------------------------

def upsert_weekly_rollup(
    rollup: WeeklyRollup,
    *,
    conn_factory=None,
) -> bool:
    """Upsert a weekly rollup row.

    Backward-compat note (PR91 amendment): the ON CONFLICT target is the
    iso_week column, which Postgres resolves against the unique partial
    index `uq_weekly_rollups_iso_week` created by the PR91 migration.
    We deliberately do NOT name a primary-key constraint here — the
    legacy primary key (if any) may differ in shape, and we are not
    allowed to touch it. The unique partial index covers exactly the
    rows whose iso_week IS NOT NULL, which is precisely the set of
    rows PR91 ever writes.

    We also do NOT write the BIGSERIAL `id` column on fresh installs.
    The fresh-install table has id BIGSERIAL PRIMARY KEY which
    auto-populates on INSERT.
    """
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return False
    if not rollup.iso_week:
        # Refuse to write a row without iso_week — the conflict target
        # cannot match a NULL value and a legacy row could be created
        # by accident. Surface as a compatibility warning instead.
        rollup.compatibility_warnings.append({
            "code":    "REFUSED_WRITE_NO_ISO_WEEK",
            "message": "upsert_weekly_rollup refused to write a row with NULL iso_week",
        })
        return False
    row = rollup.to_db_row()
    cols = list(row.keys())
    placeholders = ",".join(["%s"] * len(cols))
    update_set = ",".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "iso_week")
    sql = (
        f"INSERT INTO weekly_rollups ({','.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (iso_week) DO UPDATE SET {update_set}, updated_at=NOW()"
    )
    args: list[Any] = []
    for c in cols:
        v = row[c]
        if isinstance(v, (dict, list)):
            args.append(json.dumps(v, default=str))
        else:
            args.append(v)
    try:
        with cf() as cur:
            cur.execute(sql, args)
        return True
    except Exception as e:
        log.error("upsert_weekly_rollup failed for %s: %s", rollup.iso_week, e)
        return False


def fetch_weekly_rollup(
    iso_week: str,
    *,
    conn_factory=None,
) -> Optional[dict[str, Any]]:
    """Fetch a weekly rollup row by iso_week, tolerating legacy schemas.

    Backward-compat (PR91 amendment): if the row exists but is missing
    any PR91-introduced column, we DO NOT crash. We surface a
    compatibility_warnings list on the returned dict so the dashboard
    can flag the row and the user knows the underlying record came from
    an older PR. We never overwrite or delete legacy columns.
    """
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return None
    try:
        with cf() as cur:
            cur.execute("SELECT * FROM weekly_rollups WHERE iso_week=%s", [iso_week])
            desc = [d[0] for d in cur.description] if cur.description else []
            row = cur.fetchone()
            if not row:
                return None
            if isinstance(row, dict):
                out = dict(row)
            else:
                out = dict(zip(desc, row))
            # Attach compatibility warnings for any missing PR91 column.
            out.setdefault("compatibility_warnings", [])
            for pr91_col in (
                "iso_week", "week_start", "week_end",
                "days_present", "days_missing", "daily_index",
                "section_errors_by_day", "missing_sections_by_day",
                "source_status_by_day", "totals",
            ):
                if pr91_col not in out:
                    out["compatibility_warnings"].append({
                        "code":    "LEGACY_WEEKLY_ROLLUP_MISSING_COLUMN",
                        "column":  pr91_col,
                        "message": (f"weekly_rollups row for {iso_week} is missing "
                                    f"PR91 column '{pr91_col}'. The row predates the "
                                    f"PR91 migration; treat as partial."),
                    })
            return out
    except Exception as e:
        log.warning("fetch_weekly_rollup failed for %s: %s", iso_week, e)
        return None


# ---------------------------------------------------------------------------
# Disk export
# ---------------------------------------------------------------------------

def _default_disk_root() -> str:
    return os.environ.get("OPERATOR_DAILY_DISK_ROOT", "/home/user/workspace/data")


def write_weekly_disk_export(
    rollup: WeeklyRollup,
    *,
    daily_rows: list[dict[str, Any]],
    disk_root: str,
) -> str:
    """Mirror Supabase artifacts to disk under data/weekly/YYYY-Www/.

    For each daily row in the week we also drop the per-section JSONs
    into data/weekly/YYYY-Www/YYYY-MM-DD/ so a complete week is
    self-contained.

    _week.json is the index file.
    """
    target = Path(disk_root) / "weekly" / rollup.iso_week
    target.mkdir(parents=True, exist_ok=True)

    # Per-day folders
    for row in daily_rows:
        fd = row.get("folder_date")
        if isinstance(fd, (datetime, _date)):
            day_str = fd.isoformat()[:10]
        elif isinstance(fd, str):
            day_str = fd[:10]
        else:
            continue
        day_dir = target / day_str
        day_dir.mkdir(parents=True, exist_ok=True)
        for sec in SECTION_NAMES:
            data = _coerce_jsonb(row.get(sec), {})
            (day_dir / f"{sec}.json").write_text(
                json.dumps(data, indent=2, default=str), encoding="utf-8"
            )
        (day_dir / "summary.json").write_text(
            json.dumps({
                "date":             day_str,
                "iso_week":         rollup.iso_week,
                "generated_at":     row.get("generated_at"),
                "source_status":    row.get("source_status"),
                "summary":          _coerce_jsonb(row.get("summary"), {}),
                "section_errors":   _coerce_jsonb(row.get("section_errors"), {}),
                "missing_sections": _coerce_jsonb(row.get("missing_sections"), []),
            }, indent=2, default=str),
            encoding="utf-8",
        )

    # _week.json index
    (target / "_week.json").write_text(
        json.dumps(rollup.to_public(), indent=2, default=str),
        encoding="utf-8",
    )
    return str(target)


__all__ = [
    "WeeklyRollup",
    "build_weekly_rollup",
    "upsert_weekly_rollup",
    "fetch_weekly_rollup",
    "write_weekly_disk_export",
]
