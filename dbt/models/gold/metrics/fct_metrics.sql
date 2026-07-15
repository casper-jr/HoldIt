-- Weekly metrics at grain (ticker, snapshot_date). Replaces processor.py entirely.
-- LIVE snapshots only (is_reconstructed = false) — a reconstruction never earns a
-- score. Joins fundamentals to the price on snapshot_date, falling back to the last
-- trading day on or before it (holidays). Every ratio goes through safe_divide, so a
-- missing input is NULL, never 0.
--
-- PEG and div_growth_years, which the As-Is left null / fetch-time, are computed here
-- from the annual EPS series and fct_dividend_history — a deliberate improvement over
-- the old scorer (architecture.md, Step 5).

with fundamentals as (
    select *
    from {{ ref('fct_financials_snapshot') }}
    where not is_reconstructed
),

-- as-of price: the latest trading day on or before the snapshot
price_asof as (
    select ticker, snapshot_date, close from (
        select
            f.ticker,
            f.snapshot_date,
            p.close,
            row_number() over (
                partition by f.ticker, f.snapshot_date
                order by p.price_date desc
            ) as rn
        from fundamentals f
        join {{ ref('fct_price_daily') }} p
            on p.ticker = f.ticker
            and p.price_date <= f.snapshot_date
    )
    where rn = 1
),

-- annual EPS series -> latest year-over-year growth rate (%), for PEG
annual_eps as (
    select distinct
        ticker,
        income_period_end,
        {{ safe_divide('net_income', 'shares_outstanding') }} as eps
    from {{ ref('fct_financials_snapshot') }}
    where income_period_end is not null
),
eps_growth as (
    select ticker, {{ safe_divide('eps - prev_eps', 'abs(prev_eps)') }} * 100 as eps_growth_rate
    from (
        select
            ticker, eps,
            lag(eps) over (partition by ticker order by income_period_end) as prev_eps,
            row_number() over (partition by ticker order by income_period_end desc) as rn
        from annual_eps
    )
    where rn = 1
),

-- dividend history over completed years only (the current year is partial)
div_annual as (
    select
        ticker, fiscal_year, dividend_per_share,
        lag(dividend_per_share) over (partition by ticker order by fiscal_year) as prev_dps,
        row_number() over (partition by ticker order by fiscal_year desc) as rn_recent
    from {{ ref('fct_dividend_history') }}
    where fiscal_year < extract(year from date('{{ var("launch_date") }}'))
),
latest_dps as (
    select ticker, dividend_per_share as annual_dividend
    from div_annual where rn_recent = 1
),
div_flags as (
    select
        ticker,
        dividend_per_share > prev_dps as increased,
        row_number() over (partition by ticker order by fiscal_year desc) as rn
    from div_annual
    where prev_dps is not null
),
div_first_break as (
    select ticker, min(rn) as break_rn from div_flags where not increased group by ticker
),
div_growth as (
    select f.ticker, coalesce(min(b.break_rn) - 1, max(f.rn)) as div_growth_years
    from div_flags f
    left join div_first_break b on f.ticker = b.ticker
    group by f.ticker
),

joined as (
    select
        f.ticker,
        f.snapshot_date,
        pr.close,
        f.net_income,
        f.shares_outstanding,
        f.stockholders_equity,
        f.total_liabilities,
        f.free_cash_flow,
        g.eps_growth_rate,
        d.div_growth_years,
        ld.annual_dividend
    from fundamentals f
    left join price_asof pr on pr.ticker = f.ticker and pr.snapshot_date = f.snapshot_date
    left join eps_growth g on g.ticker = f.ticker
    left join div_growth d on d.ticker = f.ticker
    left join latest_dps ld on ld.ticker = f.ticker
),

metrics as (
    select
        ticker,
        snapshot_date,
        close,
        {{ safe_divide('close * shares_outstanding', 'net_income') }}         as per,
        {{ safe_divide('close * shares_outstanding', 'stockholders_equity') }} as pbr,
        {{ safe_divide('net_income', 'stockholders_equity') }} * 100          as roe,
        {{ safe_divide('free_cash_flow', 'close * shares_outstanding') }} * 100 as fcf_yield,
        {{ safe_divide('total_liabilities', 'stockholders_equity') }} * 100   as debt_ratio,
        {{ safe_divide('annual_dividend', 'close') }} * 100                   as dividend_yield,
        eps_growth_rate,
        coalesce(div_growth_years, 0) as div_growth_years
    from joined
)

select
    ticker,
    snapshot_date,
    close,
    per,
    pbr,
    roe,
    fcf_yield,
    debt_ratio,
    dividend_yield,
    -- PEG is only meaningful when both PER and growth are positive; otherwise NULL
    -- (a negative PER over negative growth would otherwise fake a positive PEG)
    case when per > 0 and eps_growth_rate > 0 then per / eps_growth_rate end as peg,
    div_growth_years
from metrics
