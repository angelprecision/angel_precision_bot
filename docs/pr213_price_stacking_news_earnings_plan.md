# PR 213 — Price stacking and news/earnings context

This PR is the planned scoring-book slot for the remaining observe-only modules.

## Intended files

- `ap/price_stacking.py`
- `ap/news_earnings_context.py`
- `tests/test_price_stacking.py`
- `tests/test_news_earnings_context.py`
- `docs/price_stacking.md`
- `docs/news_earnings_context.md`

## Scope

Diagnostics only. This PR must not submit orders, cancel orders, mutate queue rows, mutate order rows, mutate positions, mutate proof trades, replace scanner score, replace plan score, or alter live/paper taxonomy.

## Required behavior

Price stacking should compare scanner entry/stop/target levels against 4H, daily, weekly, and monthly levels. It should report nearby support, nearby resistance, stacked levels, stop protection, and target path friction.

News/earnings context should read provided signal/context fields only. It should report earnings risk, event risk, catalyst risk, and missing data. It must not fetch or infer news.

## Merge condition

Merge only after the actual module files and tests are added and CI passes. This document opens the slot so the scoring-book stack remains contained in PRs 209 through 214.
