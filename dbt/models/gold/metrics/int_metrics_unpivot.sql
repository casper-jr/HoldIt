-- fct_metrics unpivoted to (ticker, snapshot_date, metric, value) so a single range
-- join against seed_scoring_rules scores every metric at once. UNPIVOT drops NULLs, so
-- a missing metric simply produces no row and contributes no points — missing is not
-- zero, it is absent. div_growth_years is cast to float to share the value column.
with m as (
    select
        ticker,
        snapshot_date,
        per,
        pbr,
        roe,
        fcf_yield,
        debt_ratio,
        dividend_yield,
        peg,
        cast(div_growth_years as float64) as div_growth_years
    from {{ ref('fct_metrics') }}
)

select ticker, snapshot_date, metric, value
from m
unpivot(value for metric in (
    per, pbr, roe, fcf_yield, debt_ratio, dividend_yield, peg, div_growth_years
))
