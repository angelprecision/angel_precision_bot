# News and earnings context

`ap/news_earnings_context.py` is observe-only. It reads provided signal/context fields and returns event-risk diagnostics.

It must not fetch news, infer missing events, submit orders, cancel orders, mutate queue rows, mutate order rows, mutate position rows, mutate proof rows, replace scanner score, or replace plan score.

Future amendments should align thresholds and block-recommendation names with `ap/score_profile_config.py` from #209A.
