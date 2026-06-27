# P1: Funnel audit proof schema selection hardening

## Problem

`ap/funnel_audit.py` currently reads `proof_trades` with this column list:

```text
trade_id,client_email,signal_id,status,exit_bucket,created_at
```

The production `proof_trades` table shape has:

```text
id,client_email,created_at,exit_bucket,position_id,local_order_id
```

but does not have `trade_id`, `signal_id`, or `status`.

That means the funnel report can mark the `EXITED` source as unavailable even though proof rows exist.

## Required code fix

In `ap/funnel_audit.py`, change the proof-trades select from:

```python
columns="trade_id,client_email,signal_id,status,exit_bucket,created_at"
```

to:

```python
columns=(
    "id,client_email,position_id,local_order_id,"
    "exit_bucket,created_at"
)
```

Then change the `EXITED` count from:

```python
n_exited = sum(
    1 for p in proofs_f
    if (p.get("status") or "").upper() in ("CLOSED", "EXITED", "FILLED")
    or (p.get("exit_bucket") or "")
)
```

to:

```python
n_exited = sum(1 for p in proofs_f if p.get("exit_bucket"))
```

## Safety

- Reporting/read-only only.
- No schema migration.
- No broker submit/cancel.
- No order mutation.
- No position mutation.
- No queue mutation.
- No handoff mutation.

## Test expectation

Add a unit test that monkeypatches `_safe_select` and captures the column list requested for `proof_trades`, asserting it does not include nonexistent `trade_id`, `signal_id`, or `status`.
