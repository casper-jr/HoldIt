-- Fundamentals at grain (ticker, snapshot_date). No price column — price is a join
-- to fct_price_daily. This table is what gives the project a time dimension: the same
-- ticker carries different fundamentals as different annual filings become knowable.
--
-- Two eras, one grain:
--   live         snapshot_date >= launch_date : real weekly fetches (is_reconstructed=false)
--   reconstructed snapshot_date <  launch_date : synthetic weekly Fridays, each attached
--                to the most recent annual filing knowable on that date (is_reconstructed=true)
--
-- Both eras run through ONE as-of join: attach the annual filing whose filing_date is
-- on or before snapshot_date, most recent wins. Filing lag is an assumption for US
-- (yfinance exposes no filing date) — annual results become public us_filing_lag_days
-- after fiscal year end. That single rule is what keeps lookahead bias out; DART gives
-- real rcept_dt for KR in Step 6.

with financials as (
    -- keep only the most recent fetch per ticker; annuals do not change week to week,
    -- and the backfill fetch carries the full ~4-year history of periods.
    select f.*
    from {{ ref('stg_yf__financials') }} f
    join (
        select ticker, max(snapshot_date) as latest_snapshot
        from {{ ref('stg_yf__financials') }}
        group by ticker
    ) m
        on f.ticker = m.ticker
        and f.snapshot_date = m.latest_snapshot
),

annual_filings as (
    -- one row per (ticker, fiscal period end), the needed line-items pivoted out of the
    -- long form, with the US filing-lag assumption turned into a concrete filing_date.
    select
        ticker,
        period_end,
        date_add(period_end, interval {{ var('us_filing_lag_days') }} day) as filing_date,
        max(if(statement = 'income_stmt'  and line_item = 'Net Income',             value, null)) as net_income,
        max(if(statement = 'income_stmt'  and line_item = 'Total Revenue',          value, null)) as total_revenue,
        max(if(statement = 'income_stmt'  and line_item = 'Diluted Average Shares', value, null)) as shares_outstanding,
        max(if(statement = 'balance_sheet' and line_item = 'Stockholders Equity',   value, null)) as stockholders_equity,
        max(if(statement = 'balance_sheet' and line_item = 'Total Debt',            value, null)) as total_debt,
        max(if(statement = 'balance_sheet' and line_item = 'Total Assets',          value, null)) as total_assets,
        max(if(statement = 'balance_sheet' and line_item = 'Total Liabilities Net Minority Interest', value, null)) as total_liabilities,
        max(if(statement = 'cashflow'      and line_item = 'Free Cash Flow',        value, null)) as free_cash_flow
    from financials
    where period_end is not null
    group by ticker, period_end
),

live_snapshots as (
    -- real fetched snapshots on/after launch
    select distinct ticker, snapshot_date, false as is_reconstructed
    from {{ ref('stg_yf__financials') }}
    where snapshot_date >= date('{{ var("launch_date") }}')
),

reconstruction_grid as (
    -- synthetic weekly Fridays for the five years before launch, one per ticker
    select
        c.ticker,
        d as snapshot_date,
        true as is_reconstructed
    from (select distinct ticker from {{ ref('dim_company') }}) c
    cross join unnest(generate_date_array(
        date_sub(date('{{ var("launch_date") }}'), interval {{ var('price_history_years') }} year),
        date_sub(date('{{ var("launch_date") }}'), interval 1 day),
        interval 1 day
    )) d
    where extract(dayofweek from d) = 6   -- Friday, matching the weekly DAG cadence
),

grid as (
    select ticker, snapshot_date, is_reconstructed from live_snapshots
    union all
    select ticker, snapshot_date, is_reconstructed from reconstruction_grid
)

select
    g.ticker,
    g.snapshot_date,
    g.is_reconstructed,
    af.net_income,
    af.total_revenue,
    af.shares_outstanding,
    af.stockholders_equity,
    af.total_debt,
    af.total_assets,
    af.total_liabilities,
    af.free_cash_flow,
    af.period_end   as income_period_end,
    af.period_end   as balance_period_end,
    af.filing_date  as fundamentals_asof_date,
    'annual'        as fundamentals_granularity
from grid g
left join annual_filings af
    on af.ticker = g.ticker
    and af.filing_date <= g.snapshot_date
qualify row_number() over (
    partition by g.ticker, g.snapshot_date
    order by af.filing_date desc
) = 1
