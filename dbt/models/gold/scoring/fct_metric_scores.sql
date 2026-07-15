-- Per-metric points at grain (ticker, snapshot_date, model_version, metric). The core
-- of the "scoring as data" claim: a single range join of int_metrics_unpivot to
-- seed_scoring_rules on value >= min_value AND value < max_value scores every numeric
-- metric for every model at once. Adding a model is adding CSV rows.
--
-- Two metrics can't be pure range-join rows and are handled explicitly:
--   moat  — a human 0-10 score from seed_qualitative_moat, added directly (v2 only)
--   v2 pbr — v2's PBR score depends on ROE (a high-ROE stock earns a premium), a
--            two-variable function no single (min,max,points) row can express. It is
--            computed below and replaces the v2 pbr range-join row. v1 pbr has no such
--            interaction and stays a plain range-join row.
--
-- Everything here derives from fct_metrics, which is live-only, so no score can ever
-- trace to a reconstructed fundamentals row (assert_scores_are_live_only).

with ranged as (
    select
        u.ticker,
        u.snapshot_date,
        r.model_version,
        u.metric,
        r.points
    from {{ ref('int_metrics_unpivot') }} u
    join {{ ref('seed_scoring_rules') }} r
        on r.metric = u.metric
        and u.value >= r.min_value
        and u.value < r.max_value
    where not (r.model_version = 'v2' and u.metric = 'pbr')   -- v2 pbr computed below
),

v2_pbr as (
    select
        ticker,
        snapshot_date,
        'v2' as model_version,
        'pbr' as metric,
        case
            when pbr is null or pbr <= 0 then 0
            when roe > 20 then (case when pbr < 3.0 then 5 else least(base + 3, 5) end)
            when roe > 15 then (case when pbr < 2.0 then 5 else least(base + 2, 5) end)
            else base
        end as points
    from (
        select
            ticker, snapshot_date, pbr, roe,
            case when pbr < 0.3 then 5 when pbr < 0.6 then 4 when pbr < 1.0 then 3 else 0 end as base
        from {{ ref('fct_metrics') }}
    )
    where pbr is not null
),

moat as (
    select
        f.ticker,
        f.snapshot_date,
        sm.model_version,
        'moat' as metric,
        least(coalesce(qm.moat_points, 0), sm.max_points) as points
    from (select distinct ticker, snapshot_date from {{ ref('fct_metrics') }}) f
    join {{ ref('seed_scoring_models') }} sm on sm.metric = 'moat'
    left join {{ ref('seed_qualitative_moat') }} qm on qm.ticker = f.ticker
)

select ticker, snapshot_date, model_version, metric, points from ranged
union all
select ticker, snapshot_date, model_version, metric, points from v2_pbr
union all
select ticker, snapshot_date, model_version, metric, points from moat
