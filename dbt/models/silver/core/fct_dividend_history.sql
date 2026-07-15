-- Dividend history at grain (ticker, fiscal_year). Replaces the As-Is "consecutive
-- increase years" integer computed at fetch time — that derivation, if wanted, is now
-- a Gold query over this observed history. fiscal_year is the calendar year of the
-- ex-dividend date; the dividend payload is full history each fetch, so we keep the
-- most recent snapshot and aggregate its events by year.
--
-- No snapshot_date or date column, so the folder's snapshot_date partition is off.
{{ config(partition_by=none) }}

with dividends as (
    select d.*
    from {{ ref('stg_yf__dividends') }} d
    join (
        select ticker, max(snapshot_date) as latest_snapshot
        from {{ ref('stg_yf__dividends') }}
        group by ticker
    ) m
        on d.ticker = m.ticker
        and d.snapshot_date = m.latest_snapshot
)

select
    ticker,
    extract(year from ex_date)  as fiscal_year,
    sum(dividend)               as dividend_per_share,
    count(*)                    as num_payments
from dividends
group by ticker, fiscal_year
