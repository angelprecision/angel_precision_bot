# Chart memory confirmation

This PR adds the safe foundation for using chart screenshots as entry-confirmation memory.

## Goal

Let the bot store a structured memory row for a chart screenshot review, then use that memory before entry submit to identify conditions like:

- continuation confirmed
- clean reclaim
- fresh intraday high or low
- late chase
- failed reclaim
- chop
- unclear chart

## Safety model

Default flags keep this completely off:

```env
CHART_VISION_CONFIRMATION_ENABLED=false
CHART_VISION_BLOCK_SUBMIT=false
```

Observe-only rollout:

```env
CHART_VISION_CONFIRMATION_ENABLED=true
CHART_VISION_BLOCK_SUBMIT=false
```

Blocking rollout, only after proof:

```env
CHART_VISION_CONFIRMATION_ENABLED=true
CHART_VISION_BLOCK_SUBMIT=true
```

## Invariants

This PR does not submit orders, cancel orders, mutate positions, mutate proof_trades, or mutate trade_queue.

The module preserves `client_id`, `execution_mode`, and `signal_id` on every memory row so paper/live taxonomy cannot bleed together.

## Suggested table

Create this table in Supabase before enabling observe-only mode:

```sql
create table if not exists chart_memory_confirmations (
  id uuid primary key,
  client_id text not null,
  execution_mode text not null,
  signal_id text not null,
  ticker text not null,
  side text not null,
  timeframe text not null,
  trigger_level numeric,
  current_price numeric,
  screenshot_url text,
  chart_state text not null,
  reason text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_chart_memory_signal_latest
  on chart_memory_confirmations (client_id, execution_mode, signal_id, created_at desc);

create index if not exists idx_chart_memory_ticker_latest
  on chart_memory_confirmations (ticker, timeframe, created_at desc);
```

## Manual trade-history backfill shape

When backfilling screenshots from actual prior trades, insert one row per signal/trade review:

```json
{
  "client_id": "jasoncosby1@gmail.com",
  "execution_mode": "live",
  "signal_id": "canonical-signal-id",
  "ticker": "AAPL",
  "side": "CALL",
  "timeframe": "1d",
  "trigger_level": 201.5,
  "current_price": 202.1,
  "screenshot_url": "https://...",
  "chart_state": "continuation_confirmed",
  "reason": "Price reclaimed trigger, held above level, and extended after retest.",
  "metadata": {
    "source": "manual_backfill",
    "reviewer": "operator",
    "trade_result": "winner"
  }
}
```

For losers, use states like `late_chase`, `failed_reclaim`, `chop`, `wick_only`, or `unclear`.

## Entry-submit integration hook

The submit path should call this immediately before broker or paper submit:

```python
from ap_chart_memory_confirmation import should_allow_entry_from_chart_memory

allowed, chart_reason, chart_row = should_allow_entry_from_chart_memory(
    supabase,
    client_id=client_id,
    execution_mode=execution_mode,
    signal_id=signal_id,
)

if not allowed:
    log.warning(
        "ENTRY_BLOCKED_BY_CHART_MEMORY client_id=%s execution_mode=%s signal_id=%s reason=%s",
        client_id,
        execution_mode,
        signal_id,
        chart_reason,
    )
    return {
        "status": "REJECTED",
        "last_error": chart_reason,
        "diagnostics": {"chart_memory": chart_row},
    }
```

Do not place this in scanners. The scanner can produce ideas; the bot entry path must own final submit safety.
