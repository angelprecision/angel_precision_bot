# Price stacking context

`ap/price_stacking.py` is observe-only. It compares provided scanner/context levels near the proposed entry and returns diagnostics for nearby stacked structure.

This module must not submit orders, cancel orders, mutate queue rows, mutate order rows, mutate position rows, mutate proof rows, replace scanner score, or replace plan score.

Future amendments should expand support/resistance classification, stop protection, and target path friction after #209A config and side normalization are finalized.
