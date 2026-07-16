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
-- Currency (the ADR case): these ratios mix price (price currency) with statement figures
-- (financial currency). When they differ, the daily ratio is meaningless and there is no
-- per-day currency-consistent source, so PER/PBR/FCF-yield are left NULL. (fct_metrics
-- recovers a current PER for ADRs from Yahoo's trailingPE; that has no daily history.)
--
-- Partitioned by price_date, overriding the folder's snapshot_date default.
{{ config(partition_by={'field': 'price_date', 'data_type': 'date'}) }}

with company as (
    select ticker, financial_currency
    from {{ ref('dim_company') }}
    where is_current
),

asof as (
    select
        p.ticker,
        p.price_date,
        p.close,
        p.currency,
        c.financial_currency,
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
    left join company c on c.ticker = p.ticker
),

flagged as (
    select *,
        (currency is not null and financial_currency is not null
         and currency != financial_currency) as ccy_mismatch
    from asof
    where rn = 1
)

select
    ticker,
    price_date,
    close,
    currency,
    fundamentals_snapshot_date,
    is_reconstructed,
    case when ccy_mismatch then null
         else {{ safe_divide('close * shares_outstanding', 'net_income') }} end          as per,
    case when ccy_mismatch then null
         else {{ safe_divide('close * shares_outstanding', 'stockholders_equity') }} end as pbr,
    case when ccy_mismatch then null
         else {{ safe_divide('free_cash_flow', 'close * shares_outstanding') }} * 100 end as fcf_yield
from flagged
