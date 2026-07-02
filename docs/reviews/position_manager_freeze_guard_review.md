# Position Manager freeze guard review

Verdict: HOLD-LIGHT / mostly strong.

Scope reviewed against `main`: `ap/position_manager.py`.

This is older hardened infrastructure, not a fresh regression suspect. The file already has advisory locking for `open_position()`, execution idempotency keys, active-only dedup, one-connection snapshot reads, and terminal-linked fill suppression.

## Confirmed production-shape findings

1. `APPositionManager.__init__` stores `client_id` exactly as provided. It should normalize once with `str(client_id or "").strip().lower()` so position reads, order reads, snapshots, and advisory lock keys do not split by casing or whitespace.

2. `open_position()` validates positive quantity and positive entry price, but it persists `side.upper()` directly into `positions.direction`. This does not default to CALL, but it can store invalid direction taxonomy if an upstream repair/recovery path passes junk.

3. `PositionStatus.ACTIVE` is `{OPEN, CLOSING}` while runtime SQL treats `OPEN`, `CLOSING`, `PARTIAL`, `ACTIVE`, or remaining quantity as active. The symbolic constant should be aligned so future imports do not regress active-position accounting.

## Required source hunk before merge

Apply the production source hunk in `ap/position_manager.py`:

- Add a local `_normalize_position_side(side)` helper near the existing normalization helpers.
- Normalize known CALL/PUT aliases to canonical `CALL` or `PUT`.
- Raise `ValueError("invalid_or_missing_position_side:<raw>")` for missing/invalid side.
- Normalize `self.client_id` in `__init__`.
- In `open_position()`, compute `_side = _normalize_position_side(side)` after the qty/price checks and insert `_side` instead of `side.upper()`.
- Align `PositionStatus.ACTIVE` with runtime active statuses, or introduce shared `ACTIVE_DB_STATUSES`.

## Merge safety

Do not rewrite snapshot, advisory-lock idempotency, partial-close math, proof-trade repair, or capital accounting in this patch.

This branch currently codifies the freeze invariants in tests. It is a HOLD branch until the source hunk is applied.
