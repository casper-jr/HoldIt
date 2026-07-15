-- The screening leaderboard. Grain (ticker, snapshot_date, model_version): total and
-- normalized (0-100) score, grade, and rank within the model that week. All model
-- versions coexist in one table, distinguished by model_version — not one view each.
-- Normalizing by each model's own max (v1=47, v2=100) is what makes grades comparable
-- and fixes the As-Is bug where A>80 was applied to a scale that never reached 100.
with spine as (
    select distinct s.ticker, s.snapshot_date, m.model_version
    from (select distinct ticker, snapshot_date from {{ ref('fct_metrics') }}) s
    cross join (select distinct model_version from {{ ref('seed_scoring_models') }}) m
),

totals as (
    select ticker, snapshot_date, model_version, sum(points) as total_score
    from {{ ref('fct_metric_scores') }}
    group by 1, 2, 3
),

model_max as (
    select model_version, sum(max_points) as max_score
    from {{ ref('seed_scoring_models') }}
    group by 1
),

scored as (
    select
        sp.ticker,
        sp.snapshot_date,
        sp.model_version,
        coalesce(t.total_score, 0) as total_score,
        round(coalesce(t.total_score, 0) / mm.max_score * 100, 1) as normalized_score
    from spine sp
    left join totals t using (ticker, snapshot_date, model_version)
    join model_max mm using (model_version)
)

select
    s.ticker,
    s.snapshot_date,
    s.model_version,
    c.name,
    c.sector,
    c.market,
    s.total_score,
    s.normalized_score,
    case
        when s.normalized_score > 80 then 'A'
        when s.normalized_score >= 70 then 'B'
        when s.normalized_score >= 50 then 'C'
        else 'D'
    end as grade,
    rank() over (
        partition by s.snapshot_date, s.model_version
        order by s.normalized_score desc
    ) as rank
from scored s
left join {{ ref('dim_company') }} c
    on c.ticker = s.ticker and c.is_current
