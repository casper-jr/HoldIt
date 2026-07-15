-- Null rate and row count per metric per snapshot, a first-class table tracked over
-- time. This is the direct answer to the As-Is baseline (PEG null ~19%, div-growth zero
-- ~16%, 39 silent excepts): here the null rate is measured and visible, not hidden.
{% set metrics = ['per', 'pbr', 'roe', 'fcf_yield', 'debt_ratio', 'dividend_yield', 'peg', 'div_growth_years'] %}

with base as (
    select * from {{ ref('fct_metrics') }}
)

{% for m in metrics %}
select
    snapshot_date,
    '{{ m }}' as metric,
    count(*) as ticker_count,
    countif({{ m }} is null) as null_count,
    round(countif({{ m }} is null) / count(*), 3) as null_rate
from base
group by snapshot_date
{% if not loop.last %}union all{% endif %}
{% endfor %}
