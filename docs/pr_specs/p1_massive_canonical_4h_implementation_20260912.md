# P1 — Massive 4H Provider Boundary and Qualification Canary

## Status

**DRAFT / HARD HOLD / PREFLIGHT FIRST.** This change adds an observe-only
provider boundary and an operator canary. It does not make Massive trading
authority and does not modify signal, watcher, selector, broker, order,
position, queue, or proof behavior.

Base for this implementation branch: `main@71f25dabf739bce3360593ac44a7c7bf71ef9513`.

## Runtime contract

`ap.massive_market_data` owns only:

- environment-only `MASSIVE_API_KEY` resolution, with temporary
  `POLYGON_API_KEY` compatibility;
- header-authenticated Massive custom-aggregate transport;
- strict adjusted OHLCV normalization and deterministic source identity;
- America/New_York RTH session geometry, DST, holidays, early closes, and
  completed-bar filtering;
- native `4/hour` comparison and deterministic 15-minute RTH fallback;
- shared per-credential/per-ticker cache, singleflight refresh, and immutable
  last-good stale reads.

The default is `MASSIVE_4H_ALIGNMENT=rth_15m`. Native bars are selectable only
after the canary documents that they match the supplied chart references.
Incomplete RTH buckets are omitted rather than synthesized. Provider failure
is `UNAVAILABLE`/`STALE`, never adverse market truth.

## Live qualification

Run `scripts/massive_4h_canary.py` from an authorized app or Render runtime.
The process accepts the key only through the environment and emits sanitized
JSON. It requires an approved less-liquid AP underlying and a local,
validation-only reference file with at least 20 known chart cases. The cases
must cover multiple tickers and collectively tag `dst`, `early_close`, and
`holiday`.

Reference file shape:

```json
{
  "version": 1,
  "cases": [
    {
      "id": "spy-dst-2026-03-09",
      "ticker": "SPY",
      "from_date": "2026-03-06",
      "to_date": "2026-03-10",
      "tags": ["dst"],
      "bars": [
        {
          "started_at": "2026-03-06T14:30:00Z",
          "open": 0.0,
          "high": 0.0,
          "low": 0.0,
          "close": 0.0,
          "volume": 0.0
        }
      ],
      "fvg": [
        {
          "direction": "bullish",
          "low": 0.0,
          "high": 0.0,
          "midpoint": 0.0,
          "start_index": 0,
          "end_index": 2
        }
      ]
    }
  ]
}
```

The numeric values above are schema placeholders, not validation data. Do not
commit a reference file or a provider credential. Each case's `bars` must be
the complete canonical 4H window for that case, and `fvg` must be the exact
reference geometry for that same ordered window. The canary checks fallback
bar parity, FVG geometry parity, native-vs-fallback alignment, authenticated
history for SPY/QQQ/NVDA/MSFT plus the approved less-liquid ticker, a realistic
universe load, zero 429s, and same-ticker singleflight.

Example invocation (the key is injected by the environment/secret manager,
never placed in this command or its output):

```bash
python scripts/massive_4h_canary.py \
  --less-liquid-ticker BMY \
  --reference-file /secure/operator-only/massive_4h_reference_v1.json
```

For a plan with a five-calls-per-minute limit, set
`MASSIVE_CANARY_REQUEST_INTERVAL_SECONDS` to a pacing value appropriate for
the requested universe. A recurring 429, any missing required history, or any
reference mismatch keeps the result `HARD_HOLD`. A `PASS` report is evidence
for the provider/alignment decision only; it does not authorize merging,
deployment, or execution-path integration.

## Evidence and merge boundary

CI proves deterministic mocked transport/geometry/security behavior and exact
head/merge-ref parity. It cannot replace the authenticated provider canary or
the 20 supplied chart references. Until a sanitized live `PASS` report is
attached to the PR and independently reviewed, the branch remains Draft /
HARD HOLD and the default fallback remains non-authoritative to trading.
