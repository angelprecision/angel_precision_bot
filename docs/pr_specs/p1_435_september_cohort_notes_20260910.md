# September LIVE cohort notes

## Found in-repo
- Amendment `p1_435_regime_pullback_architecture_amendment_20260909.md` names QQQ/HOOD/LULU narratives (immediate red / pullback) without durable signal/order IDs.
- PR #435 comments document AAPL Jason LIVE `83503891-b746-4c79-a39c-19e8cd8d4cd8` with opening-window facts.

## Not found in-repo (as of tip eed1da09 search)
- Exact durable production identities for QQQ 2026-09-09, HOOD 2026-09-09, LULU 2026-09-08, NKE 2026-09-08
- Exact option BID/ASK at those breach timestamps
- GOOGL / C / IWM winner control identities

## Honesty rule
Fixtures mark missing durable fields `UNKNOWN`. Tests skip full geometry when UNKNOWN. No fabricated option quotes or outcomes enter BREACH scoring inputs.

## Next to close cohort fully
Load exact rows from production `orders` / `trade_queue` / `ap_signals` for those session dates and replace UNKNOWN fields, then re-run `tests/test_p0_435_september_live_cohort.py`.
