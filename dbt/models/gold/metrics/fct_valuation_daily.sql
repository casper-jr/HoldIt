-- Price-driven valuation ratios at grain (ticker, price_date), recomputed every day.
-- The as-of join is the point: each price_date takes the latest fct_financials_snapshot
-- row with snapshot_date <= price_date. That one join spans both eras — live
-- fundamentals after launch, reconstructed before — and is_reconstructed carries
-- through, so every downstream row (and any Tableau chart reaching before launch) stays
-- labelled. NOT scored; scoring reads fct_metrics.
--
-- dividend_yield is intentionally left to the weekly fct_metrics: adding it here needs a
-- second per-year dividend as-of join for marginal analytic value (see architecture.md).
--
-- Partitioned by price_date, overriding the folder's snapshot_date default.
{{ config(partition_by={'field': 'price_date', 'data_type': 'date'}) }}

with asof as (
    select
        p.ticker,
        p.price_date,
        p.close,
        p.currency,
        f.snapshot_date as fundamentals_snapshot_date,
        f.is_reconstructed,
        f.net_income,
        f.shares_outstanding,
        f.stockholders_equity,
        f.free_cash_flow,
        row_number() over (
            partition by p.ticker, p.price_date
            order by f.snapshot_date desc
        ) as rn
    from {{ ref('fct_price_daily') }} p
    join {{ ref('fct_financials_snapshot') }} f
        on f.ticker = p.ticker
        and f.snapshot_date <= p.price_date
)

select
    ticker,
    price_date,
    close,
    currency,
    fundamentals_snapshot_date,
    is_reconstructed,
    {{ safe_divide('close * shares_outstanding', 'net_income') }}          as per,
    {{ safe_divide('close * shares_outstanding', 'stockholders_equity') }} as pbr,
    {{ safe_divide('free_cash_flow', 'close * shares_outstanding') }} * 100 as fcf_yield
from asof
where rn = 1
