{% snapshot snap_company %}
{{
    config(
        target_schema=('holdit_silver' if target.name == 'prod' else 'holdit_silver_' ~ target.name),
        unique_key='ticker',
        strategy='check',
        check_cols=['name', 'market', 'sector', 'industry', 'currency', 'financial_currency'],
        invalidate_hard_deletes=True,
    )
}}
-- SCD2 engine behind dim_company. dbt builds history going forward: each run compares
-- the current company attributes to the stored snapshot and closes/opens a row when a
-- checked column changes. It does NOT retro-populate history from past Bronze
-- partitions — that is deliberate; company attributes rarely change and launch-forward
-- history matches the project's live-only stance. One row per ticker per attribute era.
with latest_quote as (
    select * except (rn) from (
        select
            q.*,
            row_number() over (partition by ticker order by snapshot_date desc) as rn
        from {{ ref('stg_yf__quote') }} q
    )
    where rn = 1
)

select
    l.ticker,
    coalesce(l.long_name, l.short_name)     as name,
    m.market,
    l.sector,
    l.industry,
    l.currency,
    l.financial_currency,
    -- the backfilled universe is currently-listed by construction (a live screener);
    -- delisting detection arrives with DART in Step 6.
    false                                    as is_delisted
from latest_quote l
left join {{ ref('seed_exchange_mapping') }} m
    on l.exchange_code = m.exchange_code

{% endsnapshot %}
