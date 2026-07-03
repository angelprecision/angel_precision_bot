# ap_signal_normalizer.py — Canonical signal payload normalization
# =============================================================================
# Translates raw scanner/webhook payloads into the canonical execution metadata
# fields expected by:
#   - validate_entry_metadata() in ap/entry_metadata_guard.py
#   - ap_signals DB columns (underlying_at_signal, entry_trigger, stop_price,
#     target_price)
#   - APMasterControl.evaluate() metadata guard
#
# WHY THIS MODULE EXISTS
# ──────────────────────
# Production scanners use inconsistent field names for the same semantic values.
# The metadata guard was checking a fixed set of canonical names and blocking
# execution with metadata_invalid:zero_underlying / metadata_invalid:zero_trigger
# even when the raw payload contained valid values under a different key.
#
# This module is the single authoritative place that knows about all field name
# variants used by all scanners. No other module should replicate this mapping.
#
# SCOPE
# ─────
# Called only from the signal ingestion/store path:
#   - ap/queue.py enqueue_signal()     → normalizes before trade_queue INSERT
#   - ap_signal_store.py insert_signal() → normalizes before ap_signals UPSERT
#
# NOT called from:
#   - master_control / selector / exits / positions / orders / dashboard
#
# DESIGN INVARIANTS
# ─────────────────
# 1. Never write null/zero underlying_at_signal if raw payload contains a valid
#    positive underlying price under any recognized alias.
# 2. Never write null/zero entry_trigger if raw payload contains a valid positive
#    trigger/entry value under any recognized alias.
# 3. If underlying or trigger is truly missing, do NOT write the canonical field —
#    leave it absent so the existing DATA_PENDING guard in master_control_metadata_guard
#    can classify the signal correctly at execution time.
# 4. side / direction are NEVER defaulted or inferred here (PR #233 owns that).
# 5. This module is pure: no I/O, no DB, no network. Safe to call from any thread.
# 6. Idempotent: canonical fields already present and positive are never overwritten.
# =============================================================================

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("ap.signal_normalizer")


def _pos(v: Any) -> float | None:
    """Return a positive float from v, or None if absent/zero/negative/invalid."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _nested(raw: dict, key: str) -> Any:
    """Read a nested dict value from `raw` using dot-notation key. Never raises."""
    try:
        parts = key.split(".", 1)
        if len(parts) == 1:
            return raw.get(parts[0])
        container = raw.get(parts[0])
        if isinstance(container, dict):
            return container.get(parts[1])
        return None
    except Exception:
        return None


# ── Entry trigger field resolution order ─────────────────────────────────────
# Canonical field: entry_trigger
# The metadata guard already handles: trigger_price, entry_trigger, trigger.entry,
#   entry_price, signal_entry_price (via _positive() dot-notation support).
# This normalizer adds: trigger (scalar), entry (scalar).
_ENTRY_TRIGGER_ALIASES: tuple[tuple[str, bool], ...] = (
    # (field_key_or_dotpath, is_dotpath)
    # Canonical — skip if already positive
    ("entry_trigger",      False),  # canonical → gate; skip if present
    ("trigger_price",      False),  # canonical → gate; skip if present
    # Primary scanner aliases
    ("trigger",            False),  # scalar trigger value (not nested dict)
    ("entry_price",        False),  # common scanner alias
    ("entry",              False),  # shortened scanner alias
    ("signal_entry_price", False),  # intelligence pipeline alias
    ("trigger.entry",      True),   # nested: trigger.entry
)

# ── Underlying price field resolution order ───────────────────────────────────
# Canonical field: underlying_at_signal
# The metadata guard already handles: underlying_entry, underlying_at_signal,
#   underlying_price, current_underlying_price, current_underlying,
#   signal_underlying_price, price_at_signal, trigger.current_price.
# This normalizer adds: underlying, last, close, mark,
# prior_day_close, previous_close, prev_close, prior_close.
_UNDERLYING_ALIASES: tuple[tuple[str, bool], ...] = (
    ("underlying_at_signal",          False),  # canonical → gate; skip if present
    ("underlying_price",              False),  # common alias (already in guard)
    ("underlying",                    False),  # bare scanner alias NOT in guard
    ("price",                         False),  # generic price alias (partially in store)
    ("last",                          False),  # last trade price
    ("close",                         False),  # prior close price
    ("mark",                          False),  # options mark price
    # Prior-day close prices — used by overnight Strat scanner when live quotes
    # are unavailable pre-open.  Placed after live-quote aliases so a live quote
    # always wins when both are present, but before lower-confidence inference.
    # prior_day_high / prior_day_low are intentionally NOT mapped: they are
    # range bounds, not the representative closing price, so writing either one
    # as underlying_at_signal would produce a systematically wrong value.
    ("prior_day_close",               False),  # overnight scanner alias (primary)
    ("previous_close",                False),  # variant spelling
    ("prev_close",                    False),  # short alias
    ("prior_close",                   False),  # short alias
    ("current_underlying_price",      False),  # intelligence alias
    ("current_underlying",            False),  # intelligence alias
    ("signal_underlying_price",       False),  # ledger alias
    ("price_at_signal",               False),  # OSM alias
    ("trigger.current_price",         True),   # nested: trigger.current_price
)

# ── Stop price field resolution order ────────────────────────────────────────
# Canonical field: stop_price
_STOP_ALIASES: tuple[tuple[str, bool], ...] = (
    ("stop_price",         False),  # canonical → gate; skip if present
    ("stop",               False),  # bare scanner alias
    ("stop_loss_underlying", False), # intelligence alias
    ("stop_underlying",    False),  # OSM alias
    ("trigger.stop",       True),   # nested: trigger.stop
)

# ── Target price field resolution order ──────────────────────────────────────
# Canonical field: target_price
_TARGET_ALIASES: tuple[tuple[str, bool], ...] = (
    ("target_price",           False),  # canonical → gate; skip if present
    ("target",                 False),  # bare scanner alias
    ("pt1",                    False),  # Strat nearest target
    ("take_profit_underlying", False),  # intelligence alias
    ("target_underlying",      False),  # OSM alias
    ("trigger.pt1",            True),   # nested: trigger.pt1
    ("trigger.target",         True),   # nested: trigger.target
)


def _resolve_first_positive(
    raw: dict,
    aliases: tuple[tuple[str, bool], ...],
    *,
    skip_canonical: tuple[str, ...] = (),
) -> tuple[float | None, str | None]:
    """
    Walk `aliases` in order and return (first_positive_value, source_key).

    `skip_canonical` lists field keys that mean "already canonical — stop
    here rather than overwriting with a lower-priority alias". If the first
    match is in skip_canonical, returns the value under that key (trusting
    the existing canonical field) without returning a `source_key` to
    indicate normalization happened.

    Returns (None, None) if no positive value found.
    """
    for key, is_dotpath in aliases:
        if is_dotpath:
            val = _pos(_nested(raw, key))
        else:
            # Guard: scalar `trigger` must not be a dict
            raw_val = raw.get(key)
            if key == "trigger" and isinstance(raw_val, dict):
                continue
            val = _pos(raw_val)
        if val is not None:
            if key in skip_canonical:
                # Already canonical — preserve as-is, signal no normalization needed
                return val, None
            return val, key
    return None, None


def normalize_raw_signal_payload(raw: dict) -> dict:
    """
    Normalize a raw scanner/webhook signal payload into canonical execution fields.

    Resolves field name variants into canonical DB/execution fields.
    Returns a new dict — never mutates the input.

    Field mappings (in priority order within each group):
        entry_trigger:
            trigger (scalar) | entry_price | entry | signal_entry_price | trigger.entry
        underlying_at_signal:
            underlying | price | last | close | mark |
            prior_day_close | previous_close | prev_close | prior_close |
            current_underlying_price | current_underlying |
            signal_underlying_price | price_at_signal | trigger.current_price
        stop_price:
            stop | stop_loss_underlying | stop_underlying | trigger.stop
        target_price:
            target | pt1 | take_profit_underlying | target_underlying |
            trigger.pt1 | trigger.target

    Preserved unchanged:
        ticker, side, direction, timeframe, pattern, score,
        client_id, execution_mode, signal_id, and all other fields.

    Invariants:
        1. Canonical fields already present and positive are NEVER overwritten.
        2. Normalization is only applied when the canonical field is absent/zero.
        3. side is never defaulted (PR #233).
        4. Returns original dict unchanged if input is not a dict.

    Structured audit log:
        When normalization resolves at least one field via a non-canonical alias,
        logs SIGNAL_CANONICAL_METADATA_NORMALIZED with the fields resolved and
        their source keys for operator visibility.
    """
    if not isinstance(raw, dict):
        return raw

    out = dict(raw)
    _source_keys_used: list[str] = []

    # ── 1. entry_trigger ─────────────────────────────────────────────────────
    _TRIGGER_CANONICAL = ("entry_trigger", "trigger_price")
    _existing_trigger = max(
        (_pos(raw.get(k)) or 0.0) for k in _TRIGGER_CANONICAL
    )
    if not _existing_trigger:
        val, src = _resolve_first_positive(
            raw,
            _ENTRY_TRIGGER_ALIASES,
            skip_canonical=_TRIGGER_CANONICAL,
        )
        if val is not None and src is not None:
            out["entry_trigger"] = val
            _source_keys_used.append(f"entry_trigger←{src}")

    # ── 2. underlying_at_signal ───────────────────────────────────────────────
    _UNDERLYING_CANONICAL = ("underlying_at_signal", "underlying_price")
    _existing_underlying = max(
        (_pos(raw.get(k)) or 0.0) for k in _UNDERLYING_CANONICAL
    )
    if not _existing_underlying:
        val, src = _resolve_first_positive(
            raw,
            _UNDERLYING_ALIASES,
            skip_canonical=_UNDERLYING_CANONICAL,
        )
        if val is not None and src is not None:
            out["underlying_at_signal"] = val
            _source_keys_used.append(f"underlying_at_signal←{src}")

    # ── 3. stop_price ─────────────────────────────────────────────────────────
    _STOP_CANONICAL = ("stop_price",)
    if not _pos(raw.get("stop_price")):
        val, src = _resolve_first_positive(
            raw,
            _STOP_ALIASES,
            skip_canonical=_STOP_CANONICAL,
        )
        if val is not None and src is not None:
            out["stop_price"] = val
            _source_keys_used.append(f"stop_price←{src}")

    # ── 4. target_price ───────────────────────────────────────────────────────
    _TARGET_CANONICAL = ("target_price",)
    if not _pos(raw.get("target_price")):
        val, src = _resolve_first_positive(
            raw,
            _TARGET_ALIASES,
            skip_canonical=_TARGET_CANONICAL,
        )
        if val is not None and src is not None:
            out["target_price"] = val
            _source_keys_used.append(f"target_price←{src}")

    # ── Structured audit log ─────────────────────────────────────────────────
    if _source_keys_used:
        log.info(
            "SIGNAL_CANONICAL_METADATA_NORMALIZED "
            "signal_id=%s ticker=%s source_keys_used=%s "
            "entry_trigger=%s underlying_at_signal=%s execution_mode=%s",
            raw.get("signal_id"),
            raw.get("ticker"),
            _source_keys_used,
            out.get("entry_trigger"),
            out.get("underlying_at_signal"),
            raw.get("execution_mode"),
        )

    return out


__all__ = ["normalize_raw_signal_payload"]
