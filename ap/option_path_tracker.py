# ap/option_path_tracker.py — Angel Precision post-entry option-path tracker
# =============================================================================
# CAPTURE-ONLY. This module maintains the realized option-price path for a
# filled position and upserts it into `ap_signal_option_outcomes` keyed by
# signal_id, so the intelligence dataset can answer "how far did this setup's
# option actually run, and how fast?" — the outcome half of the edge dataset.
#
# INVARIANTS (see docs/pr_specs/intelligence_outcome_capture_20260903.md):
#   * It NEVER raises into the trading path. Every public entrypoint is
#     wrapped; on any error it logs and returns, changing nothing.
#   * It NEVER writes zeros or fabricated values. If a sample is unavailable,
#     it records nothing and leaves prior values intact.
#   * It reuses marks already polled by the caller (the exit monitor). It does
#     NOT open a new quote budget or fetch on its own.
#   * All writes are idempotent upserts on signal_id (running max/min).
# =============================================================================
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.option_path_tracker")

# hit thresholds we persist as booleans (percent gain over entry mark)
_HIT_THRESHOLDS = (10, 20, 30, 50, 100)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_pos_float(value: Any) -> Optional[float]:
    """Return a strictly-positive float, else None. Never coerces junk to 0."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f <= 0.0:  # NaN or non-positive is not a usable mark
        return None
    return f


@dataclass
class _PathState:
    """Running state for one tracked position. In-memory; DB holds the truth."""
    signal_id: str
    entry_mark: float
    entry_ts: datetime
    peak_mark: float
    trough_mark: float
    peak_ts: datetime
    trough_ts: datetime
    close_mark: Optional[float] = None
    thresholds_hit: set[int] = field(default_factory=set)


class OptionPathTracker:
    """
    Tracks the option mark path per signal_id across the position's life.

    Usage (all fail-soft):
        tracker.begin(signal_id, entry_mark)          # at fill
        tracker.observe(signal_id, current_mark)      # each monitor tick
        tracker.finalize(signal_id, close_mark)       # at position close
    """

    def __init__(self, signal_store=None):
        self._store = signal_store
        self._paths: dict[str, _PathState] = {}

    # ── lifecycle ────────────────────────────────────────────────────────────
    def begin(self, signal_id: str, entry_mark: Any) -> None:
        try:
            sid = str(signal_id or "").strip()
            em = _as_pos_float(entry_mark)
            if not sid or em is None:
                return  # no signal_id or no real entry mark → capture nothing
            now = _now_utc()
            self._paths[sid] = _PathState(
                signal_id=sid, entry_mark=em, entry_ts=now,
                peak_mark=em, trough_mark=em, peak_ts=now, trough_ts=now,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("option_path_tracker.begin failed (ignored): %s", exc)

    def observe(self, signal_id: str, current_mark: Any) -> None:
        try:
            sid = str(signal_id or "").strip()
            st = self._paths.get(sid)
            cm = _as_pos_float(current_mark)
            if st is None or cm is None:
                return  # unknown signal or unusable sample → leave state intact
            now = _now_utc()
            changed = False
            if cm > st.peak_mark:
                st.peak_mark, st.peak_ts, changed = cm, now, True
            if cm < st.trough_mark:
                st.trough_mark, st.trough_ts, changed = cm, now, True
            move_pct = (cm - st.entry_mark) / st.entry_mark * 100.0
            for thr in _HIT_THRESHOLDS:
                if thr not in st.thresholds_hit and move_pct >= thr:
                    st.thresholds_hit.add(thr)
                    changed = True
            if changed:
                self._flush(st)
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("option_path_tracker.observe failed (ignored): %s", exc)

    def finalize(self, signal_id: str, close_mark: Any) -> None:
        try:
            sid = str(signal_id or "").strip()
            st = self._paths.get(sid)
            if st is None:
                return
            cm = _as_pos_float(close_mark)
            if cm is not None:
                st.close_mark = cm
                # a close mark can itself be the peak/trough
                self.observe(sid, cm)
            self._flush(st, final=True)
            self._paths.pop(sid, None)
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("option_path_tracker.finalize failed (ignored): %s", exc)

    # ── derived outcome row ───────────────────────────────────────────────────
    def _outcome_row(self, st: _PathState, final: bool) -> dict[str, Any]:
        max_move = (st.peak_mark - st.entry_mark) / st.entry_mark * 100.0
        max_dd = (st.trough_mark - st.entry_mark) / st.entry_mark * 100.0
        mins_to_peak = round((st.peak_ts - st.entry_ts).total_seconds() / 60.0, 2)
        mins_to_trough = round((st.trough_ts - st.entry_ts).total_seconds() / 60.0, 2)
        row: dict[str, Any] = {
            "peak_mark": round(st.peak_mark, 4),
            "trough_mark": round(st.trough_mark, 4),
            "max_option_move_pct": round(max_move, 2),
            "max_option_drawdown_pct": round(max_dd, 2),
            "minutes_to_option_peak": mins_to_peak,
            "minutes_to_option_trough": mins_to_trough,
        }
        for thr in _HIT_THRESHOLDS:
            row[f"hit_{thr}pct"] = thr in st.thresholds_hit
        if final and st.close_mark is not None:
            row["close_mark"] = round(st.close_mark, 4)
        return row

    def _flush(self, st: _PathState, final: bool = False) -> None:
        if self._store is None:
            return
        try:
            self._store.insert_option_outcome(st.signal_id, self._outcome_row(st, final))
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("option_path_tracker flush failed (ignored): %s", exc)
