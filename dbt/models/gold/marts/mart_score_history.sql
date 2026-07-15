-- Score trend across snapshots per (ticker, model_version). The rising/falling/
-- consistent buckets that bq_analytics.py computed in Python become a model here. With
-- weekly snapshots this fills out over time; the week-over-week delta answers "which
-- stocks moved most in score" (Analysis Question 4).
with lb as (
    select ticker, snapshot_date, model_version, name, normalized_score, grade
    from {{ ref('mart_leaderboard') }}
)

select
    ticker,
    snapshot_date,
    model_version,
    name,
    normalized_score,
    grade,
    lag(normalized_score) over w as prev_score,
    round(normalized_score - lag(normalized_score) over w, 1) as score_delta,
    case
        when lag(normalized_score) over w is null then 'new'
        when normalized_score > lag(normalized_score) over w then 'rising'
        when normalized_score < lag(normalized_score) over w then 'falling'
        else 'consistent'
    end as trend
from lb
window w as (partition by ticker, model_version order by snapshot_date)
