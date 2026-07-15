-- "Compare models by SQL," delivered. One row per (ticker, snapshot_date) with each
-- model's normalized score, rank, and grade side by side, the rank delta, and the
-- snapshot-level Spearman rank correlation (Pearson on ranks) between v1 and v2. The
-- per-metric driver of a disagreement is a drill-down into fct_metric_scores.
with lb as (
    select * from {{ ref('mart_leaderboard') }}
),

pivoted as (
    select
        ticker,
        snapshot_date,
        max(name)   as name,
        max(sector) as sector,
        max(if(model_version = 'v1', normalized_score, null)) as v1_score,
        max(if(model_version = 'v2', normalized_score, null)) as v2_score,
        max(if(model_version = 'v1', rank, null))  as v1_rank,
        max(if(model_version = 'v2', rank, null))  as v2_rank,
        max(if(model_version = 'v1', grade, null)) as v1_grade,
        max(if(model_version = 'v2', grade, null)) as v2_grade
    from lb
    group by 1, 2
)

select
    *,
    v1_rank - v2_rank as rank_delta,
    round(corr(cast(v1_rank as float64), cast(v2_rank as float64))
        over (partition by snapshot_date), 3) as rank_corr_spearman
from pivoted
