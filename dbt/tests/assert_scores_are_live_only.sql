-- The boundary backtest.py never drew: a reconstruction may inform charts, never earn
-- a grade. Every fct_metric_scores row must trace to a live fundamentals snapshot.
-- Returns any score row whose (ticker, snapshot_date) is a reconstructed fundamentals
-- row — which must be none.
select
    s.ticker,
    s.snapshot_date,
    s.model_version,
    s.metric
from {{ ref('fct_metric_scores') }} s
join {{ ref('fct_financials_snapshot') }} f
    on f.ticker = s.ticker
    and f.snapshot_date = s.snapshot_date
where f.is_reconstructed
