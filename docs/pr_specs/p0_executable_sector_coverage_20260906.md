# P0: Canonical executable-underlying sector coverage

## STATUS

**HARD HOLD / SPEC ONLY. IMPLEMENT SURGICALLY. DO NOT MERGE FROM THIS SPEC COMMIT.**

Base at spec creation:

```text
main@9c719d72b2b9c3dba7f8c883d118142f48ce3246
```

This PR is the prerequisite coverage repair for PR #548. It owns canonical sector-map completeness only. PR #548 continues to own removal of Master Control's duplicate sector map and synthetic `other` risk authority.

---

## 1. Business objective

Angel Precision needs valid trades to stop being suppressed by false sector identity while preserving real concentration controls.

The current system has two bad states:

1. Master Control historically collapses unknown names into synthetic `other`, causing unrelated positions to block one another.
2. The canonical `ap.exposure_gate.SECTOR_MAP` does not cover a large portion of symbols that can reach execution, so removing `other` without first completing canonical coverage would make many legitimate executable names sector-cap-free.

The end state must be:

```text
known executable underlying
-> exactly one intentional canonical sector identity
-> real same-sector exposure aggregates
-> unrelated sectors do not aggregate

truly unknown/ad-hoc underlying
-> sector=None
-> no synthetic shared bucket
-> ticker / total / broker-capital / count / all other independent gates remain active
```

This PR must improve tradeflow by eliminating false cross-sector collisions, **not** by weakening risk percentages, entry quality, sizing, broker authority, or any other independent gate.

---

## 2. Scope

### Expected production file

Exactly:

```text
ap/exposure_gate.py
```

### Expected supporting files

```text
tests/test_p0_executable_sector_coverage.py
.github/workflows/p0_regression.yml
docs/pr_specs/p0_executable_sector_coverage_20260906.md
```

If any other production file is required, STOP and justify it before changing scope.

### Explicitly out of scope

Do not modify:

```text
ap_master_control.py
ap/position_manager.py
ap/order_state_machine.py
ap_reconciler.py
ap_exit_engine.py
ap/exit_safety.py
ap/brokers/tradier.py
contract selector behavior
scanner behavior
signal generation
score thresholds
entry eligibility
position sizing
max_sector_pct
max_ticker_pct
max_total_capital_pct
MAX_OPEN_PER_SYMBOL
MAX_OPEN_PER_SECTOR
broker submit
broker cancel
order mutation
position mutation
proof_trades
queue semantics
exit behavior
```

No schema migration.

No environment-variable changes.

No deployment change.

---

## 3. Binding relationship to PR #548

PR #548 currently adds BMY, NEE and PEP to `ap.exposure_gate.SECTOR_MAP` while also changing Master Control to consume the canonical resolver.

This coverage PR must take ownership of **all map additions**, including:

```text
BMY -> HEALTHCARE
NEE -> UTILITIES
PEP -> CONSUMER
```

After this PR is merged, #548 must rebase and drop those three map additions from its own diff. #548 should then contain only the Master Control identity repair plus its tests/spec/CI registration.

Do not duplicate canonical map ownership across both PRs.

---

## 4. Do not use scanner membership blindly as execution authority

`ap/scanner_utils.py` currently includes `^GSPC` in `TICKERS`, but that is a benchmark/source symbol, not an option contract underlying to be submitted directly.

Conversely, `_ZERO_DTE` contains executable index-option underlyings `SPX` and `NDX`, which are not present in `TICKERS`.

The coverage invariant therefore must be based on **symbols that can actually reach entry execution**, not simply every scanner/source symbol.

Known examples that require explicit audit:

```text
SPX
NDX
DIA
COIN
MSTR
```

`COIN` is referenced in Master Control admission flows/tests and `MSTR` is present in contract-selector executable premium policy. They must not be missed merely because they are absent from `scanner_utils.TICKERS`.

Before implementation is considered complete, audit all production entry-producing symbol authorities and construct one test-time executable-underlying set from actual production sources. At minimum review:

```text
ap/scanner_utils.py::TICKERS
ap/scanner_utils.py::_ZERO_DTE
ap_master_control.py::_PRIORITY_TICKERS
ap_master_control.py::_PREMIUM_ESTIMATES keys
ap/contract_selector.py::TICKER_MAX_PREMIUM_PER_CONTRACT keys
any other production source that can emit/admit a ticker into Master Control
```

Do not infer that every symbol found anywhere in the repository is executable. Tests, docs, benchmarks and historical examples are not authority.

Known scan-only benchmark aliases such as caret-prefixed market data symbols must be excluded from the required map **only after proving they are normalized before entry execution**.

---

## 5. Canonical sector vocabulary

Preserve every existing mapped identity on main. Do not reclassify existing names in this PR, even where a different external taxonomy could be argued.

Allowed existing/new canonical buckets for this PR:

```text
AUTO
CONSUMER
ENERGY
FINANCIAL
HEALTHCARE
INDEX
INDUSTRIAL
TECH
UTILITIES
COMMUNICATION
REAL_ESTATE
MATERIALS
CRYPTO
```

New buckets are intentional. They avoid increasing false concentration by dumping communication, real-estate, materials or crypto exposures into unrelated broad buckets merely to avoid adding a category.

Do not change cap percentages by sector. The current cap machinery compares sector identity generically and does not require a fixed hardcoded allow-list.

---

## 6. Required map additions

The following mapping is the proposed binding starting point. Validate every symbol against the actual executable universe before committing. If a symbol is proven non-executable, document why it is omitted. If an executable symbol is missing from this list, add it only after identifying the production path that can reach execution.

### HEALTHCARE

```text
ABT
AMGN
BMY
CVS
DHR
GILD
ISRG
MDT
VRTX
REGN
GEHC
DXCM
IDXX
BIIB
ILMN
MRNA
```

### FINANCIAL

```text
AIG
BK
BLK
COF
MET
USB
PYPL
HOOD
BRK.B
```

### TECH

```text
ACN
CSCO
IBM
INTU
TXN
ASML
AMAT
ADI
LRCX
SNPS
CDNS
KLAC
CRWD
FTNT
ADSK
NXPI
WDAY
MCHP
ANSS
ZS
DDOG
MDB
CDW
DELL
TEAM
ON
GFS
CTSH
ZM
```

### COMMUNICATION

```text
T
VZ
TMUS
CMCSA
CHTR
WBD
SIRI
EA
TTWO
```

### CONSUMER

```text
CL
KO
MDLZ
MO
PG
PM
PEP
MNST
KDP
KHC
CCEP
ORLY
ROST
DLTR
WBA
MAR
BKNG
DASH
MELI
PDD
```

### INDUSTRIAL

```text
HON
MMM
EMR
GD
UNP
UPS
FDX
CSX
CTAS
FAST
ODFL
CPRT
ROP
PCAR
VRSK
PAYX
ADP
CSGP
```

### UTILITIES

```text
NEE
DUK
SO
AEP
EXC
XEL
```

### ENERGY

```text
BKR
FANG
```

### REAL_ESTATE

```text
AMT
SPG
```

### MATERIALS

```text
LIN
DOW
```

### INDEX

```text
SPX
NDX
```

`SPY`, `QQQ`, `IWM` and `DIA` are already INDEX on main and must remain unchanged.

Do **not** add `^GSPC` merely because it appears in scanner data. It is a benchmark alias unless an audit proves it can reach entry execution without normalization.

### CRYPTO

```text
COIN
MSTR
```

Use CRYPTO as an operational exposure bucket for these two high-correlation crypto-sensitive option underlyings rather than incorrectly aggregating them with broad TECH.

---

## 7. Existing mappings are frozen in this PR

Do not move existing entries such as:

```text
NFLX
DIS
AMZN
META
GOOG
GOOGL
```

Even if an external taxonomy would place some of them differently.

Changing an existing sector identity changes which live positions block which future trades. That is a separate behavior change and must not ride inside a coverage-completion PR.

This PR is additive coverage only.

---

## 8. Runtime behavior must remain unchanged except identity availability

`get_sector(symbol)` must continue to:

```text
strip whitespace
uppercase
return exact mapped sector when known
return None when genuinely unmapped
```

Do not change unknown behavior to fail closed.

Do not create `OTHER`, `UNKNOWN`, `UNMAPPED`, `MISC`, `sector_unknown`, or any other shared synthetic risk bucket.

Unknown telemetry may continue to report `sector_unknown`, but that label must never become exposure authority.

---

## 9. Tradeflow requirement

This PR is not allowed to reduce valid tradeflow through accidental over-grouping.

Therefore:

1. Use the narrow canonical sectors above instead of placing all newly mapped names into broad TECH/CONSUMER/INDUSTRIAL buckets when an intentional independent sector exists.
2. Do not change `max_sector_pct` or count caps here.
3. Do not change same-ticker, total-capital, broker-capital, position-count or score gates.
4. Do not add a new rejection reason.
5. Do not add any pre-admission check.
6. Unknown ad-hoc symbols remain allowed through the sector layer and are still governed by all independent gates.

The expected tradeflow benefit is removal of false synthetic cross-sector collisions once #548 consumes this completed canonical map.

Any later evidence that the legitimate sector cap itself is too restrictive belongs in a separate measured risk-policy PR after replaying production candidates.

---

## 10. Mandatory coverage invariant

Create an executed test that derives the set of **production executable underlyings** and proves:

```text
for each executable underlying:
    get_sector(symbol) is not None
```

Do not make the test pass by hardcoding the exact same map keys as the production map.

The test must derive execution candidates independently from actual production sources.

Also assert:

```text
no mapped sector value is blank
no mapped symbol is blank
all mapped symbols are canonical uppercase/expected symbol form
no mapped value equals OTHER / UNKNOWN / MISC / UNMAPPED
```

If there are intentional executable exceptions, they must be an explicit, minimal allow-list with a production-path explanation. Preferred final state is no executable exceptions.

---

## 11. Positive identity tests

At minimum execute:

```text
BMY -> HEALTHCARE
NEE -> UTILITIES
PEP -> CONSUMER
SPX -> INDEX
NDX -> INDEX
COIN -> CRYPTO
MSTR -> CRYPTO
T -> COMMUNICATION
VZ -> COMMUNICATION
AMT -> REAL_ESTATE
LIN -> MATERIALS
CSCO -> TECH
ABT -> HEALTHCARE
HON -> INDUSTRIAL
```

Case and whitespace:

```text
" csco " -> TECH
"nee" -> UTILITIES
" spx " -> INDEX
```

Unknown controls:

```text
ZZUNKNOWN1 -> None
ZZUNKNOWN2 -> None
None -> None
"" -> None
"   " -> None
```

Use synthetic unknowns only. Never use a real production executable name as an unknown fixture simply because it happens to be missing from the map.

---

## 12. Same-sector and cross-sector controls

Execute behavior through the real exposure-gate resolver/counting path.

Required same-sector controls:

```text
UNH + BMY -> HEALTHCARE aggregation
WMT + PEP -> CONSUMER aggregation
T + VZ -> COMMUNICATION aggregation
SPY + SPX -> INDEX aggregation
COIN + MSTR -> CRYPTO aggregation
AMT + SPG -> REAL_ESTATE aggregation
LIN + DOW -> MATERIALS aggregation
```

Required cross-sector controls:

```text
QQQ + BMY -> must NOT aggregate
QQQ + NEE -> must NOT aggregate
CSCO + KO -> must NOT aggregate
T + AAPL -> must NOT aggregate
AMT + JPM -> must NOT aggregate
COIN + NVDA -> must NOT aggregate
```

The test should prove sector identity only. Give independent capital/count headroom where needed so another gate does not obscure the sector assertion.

---

## 13. Existing-behavior preservation tests

Prove existing mappings are byte-for-byte or semantically unchanged for representative controls:

```text
AAPL -> TECH
QCOM -> TECH
TSLA -> AUTO
JPM -> FINANCIAL
UNH -> HEALTHCARE
XOM -> ENERGY
WMT -> CONSUMER
BA -> INDUSTRIAL
SPY -> INDEX
QQQ -> INDEX
IWM -> INDEX
DIA -> INDEX
```

No existing mapping may be silently removed or reassigned.

---

## 14. Money-path safety

Static and behavioral audit must show this PR introduces:

```text
broker submit calls: 0
broker cancel calls: 0
order writes: 0
position writes: 0
proof_trades writes: 0
queue writes: 0
scanner logic changes: 0
selector logic changes: 0
sizing changes: 0
score changes: 0
exit changes: 0
```

This PR changes lookup data only.

---

## 15. PAPER / LIVE parity

Sector identity is market-symbol identity, not execution-mode identity.

Prove representative symbols resolve identically under LIVE and PAPER callers.

No environment variable or runner mode may change sector classification.

---

## 16. Review for hidden duplicate sector authorities

Search production code for:

```text
SECTOR_MAP
get_sector
sector_unknown
"other"
sector cap
sector exposure
```

This PR must not create a second map.

Do not modify unrelated duplicate maps in intelligence/reporting modules unless they are actual money-path sector authority. Record them as non-authoritative follow-up observations instead.

The only canonical exposure-risk map after #548 must be `ap.exposure_gate.SECTOR_MAP`.

---

## 17. CI

Add the focused test to the blocking P0 workflow in the smallest existing location.

Run:

```text
pytest tests/test_p0_executable_sector_coverage.py
pytest tests/test_p0_master_control_sector_identity.py   # after #548 rebase/integration, not by weakening tests here
exact-head blocking P0
```

For this PR itself, run neighboring exposure/risk tests that exercise `ap.exposure_gate`.

All CI evidence must be exact-head.

Do not claim skipped/mock-only tests as production proof.

---

## 18. Production replay requirement before READY

Using recent production-shaped candidates/positions or faithful fixtures, compare identity outcomes before vs after coverage.

Report at least:

```text
unmapped executable symbols before
unmapped executable symbols after
same-sector aggregations newly enabled
cross-sector false aggregations introduced: must be 0
existing sector identities changed: must be 0
```

Do not tune cap percentages in response to this replay inside this PR.

If the completed map reveals that the existing sector cap itself suppresses too many otherwise-valid trades, record that evidence for a separate PR.

---

## 19. Completion report

Before changing this PR from HARD HOLD / SPEC ONLY, update the PR body with:

```text
BASE SHA
HEAD SHA

PRODUCTION FILES
SUPPORTING FILES

EXECUTABLE UNIVERSE SOURCES AUDITED
<exact sources>

EXECUTABLE SYMBOL COUNT
<exact count>

UNMAPPED BEFORE
<count + list>

UNMAPPED AFTER
0 preferred

ADDITIVE MAPPINGS
<exact map additions>

EXISTING MAPPINGS CHANGED
0

SYNTHETIC SHARED UNKNOWN AUTHORITY
absent

SAME-SECTOR CONTROLS
<results>

CROSS-SECTOR CONTROLS
<results>

LIVE/PAPER PARITY
<results>

MONEY-PATH MUTATIONS ADDED
0

FOCUSED TESTS
<command/results>

NEIGHBORING TESTS
<command/results>

EXACT-HEAD P0
<sha + result>

FINAL DIFF
<exact files>

STATUS
READY FOR INDEPENDENT AUDIT

DO NOT MERGE
```

---

## 20. Final implementation rule

Keep this boring.

The desired production diff is a completed canonical lookup table in one file, backed by tests that prove every executable underlying has intentional identity.

Do not use this PR as an excuse to redesign exposure policy.

Correct identity first. Measure legitimate cap impact second. Tune policy only in a separate PR with production replay evidence.
