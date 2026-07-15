-- Every fundamentals row dated before launch is reconstructed and must say so —
-- is_reconstructed is what stops a pre-launch estimate from ever earning a score
-- downstream. Returns any pre-launch row that is not labelled.
select
    ticker,
    snapshot_date,
    is_reconstructed
from {{ ref('fct_financials_snapshot') }}
where snapshot_date < date('{{ var("launch_date") }}')
    and not is_reconstructed
