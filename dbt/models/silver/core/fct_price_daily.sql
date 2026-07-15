-- The ONLY price source in the warehouse. Grain (ticker, price_date), where
-- price_date comes from the payload index — never snapshot_date. A price_date can be
-- observed in more than one snapshot (the 5-year bulk load and later weekly fetches
-- overlap), so we keep the most recent snapshot's observation. is_backfilled marks a
-- row pulled historically (fetch lag beyond the 7-day weekly window) rather than live.
--
-- Partitioned by price_date, overriding the folder's snapshot_date default: this table
-- has no snapshot_date column, and price_date is what every price query filters on.
{{ config(partition_by={'field': 'price_date', 'data_type': 'date'}) }}

with latest_per_day as (
    select * except (rn) from (
        select
            p.ticker,
            p.price_date,
            p.snapshot_date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.adj_close,
            p.volume,
            row_number() over (
                partition by p.ticker, p.price_date
                order by p.snapshot_date desc
            ) as rn
        from {{ ref('stg_yf__price_history') }} p
        -- a row with no close is not a price observation: yfinance emits an
        -- incomplete-most-recent-bar (open/high/volume but null close) for the fetch
        -- day. fct_price_daily is days with an actual close.
        where p.close is not null
    )
    where rn = 1
)

select
    l.ticker,
    l.price_date,
    l.open,
    l.high,
    l.low,
    l.close,
    l.adj_close,
    l.volume,
    c.currency,
    date_diff(l.snapshot_date, l.price_date, day) > 7 as is_backfilled
from latest_per_day l
left join {{ ref('dim_company') }} c
    on l.ticker = c.ticker
    and c.is_current
