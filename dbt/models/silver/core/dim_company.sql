-- SCD2 company dimension, read straight from snap_company. valid_from/valid_to bound
-- each attribute era; is_current flags the live row. Grain (ticker, valid_from). No
-- snapshot_date here, so the folder's snapshot_date partition is overridden off — the
-- table is one row per ticker per era, tiny, and needs no partitioning.
{{ config(partition_by=none) }}

select
    ticker,
    name,
    market,
    sector,
    industry,
    currency,
    financial_currency,
    is_delisted,
    dbt_valid_from                  as valid_from,
    dbt_valid_to                    as valid_to,
    dbt_valid_to is null            as is_current
from {{ ref('snap_company') }}
