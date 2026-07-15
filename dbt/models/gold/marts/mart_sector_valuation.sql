-- Sector-relative valuation (Analysis Question 1): each stock's PER/PBR/ROE against its
-- sector's current median and against the sector's own trailing 3-year median. Both
-- halves of the ratio backfill — prices exactly, fundamentals by reconstruction — so
-- includes_reconstructed flags any sector whose 3-year window reaches before launch. An
-- uncaptioned reconstructed median is a lie; this column is how Tableau captions it.
with current_metrics as (
    select
        m.ticker,
        m.snapshot_date,
        c.sector,
        c.name,
        m.per,
        m.pbr,
        m.roe
    from {{ ref('fct_metrics') }} m
    join {{ ref('dim_company') }} c on c.ticker = m.ticker and c.is_current
    where m.snapshot_date = (select max(snapshot_date) from {{ ref('fct_metrics') }})
),

sector_current as (
    select
        sector,
        approx_quantiles(per, 2)[offset(1)] as sector_per_median,
        approx_quantiles(pbr, 2)[offset(1)] as sector_pbr_median,
        approx_quantiles(roe, 2)[offset(1)] as sector_roe_median
    from current_metrics
    group by sector
),

sector_3yr as (
    select
        c.sector,
        approx_quantiles(v.per, 2)[offset(1)] as sector_per_median_3yr,
        approx_quantiles(v.pbr, 2)[offset(1)] as sector_pbr_median_3yr,
        logical_or(v.is_reconstructed) as includes_reconstructed
    from {{ ref('fct_valuation_daily') }} v
    join {{ ref('dim_company') }} c on c.ticker = v.ticker and c.is_current
    where v.price_date >= date_sub(
        (select max(price_date) from {{ ref('fct_valuation_daily') }}), interval 3 year
    )
    group by c.sector
)

select
    cm.ticker,
    cm.snapshot_date,
    cm.sector,
    cm.name,
    cm.per,
    cm.pbr,
    cm.roe,
    sc.sector_per_median,
    sc.sector_pbr_median,
    sc.sector_roe_median,
    s3.sector_per_median_3yr,
    s3.sector_pbr_median_3yr,
    s3.includes_reconstructed,
    round({{ safe_divide('cm.per', 'sc.sector_per_median') }}, 2)     as per_vs_sector,
    round({{ safe_divide('cm.per', 's3.sector_per_median_3yr') }}, 2) as per_vs_sector_3yr,
    round({{ safe_divide('cm.pbr', 's3.sector_pbr_median_3yr') }}, 2) as pbr_vs_sector_3yr
from current_metrics cm
left join sector_current sc using (sector)
left join sector_3yr s3 using (sector)
