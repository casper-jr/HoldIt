-- Lookahead guard: no fundamentals row may be built from a filing that was not yet
-- knowable on its snapshot_date. A failure here means future information leaked into
-- the past and every reconstructed valuation is corrupted. Returns offending rows.
select
    ticker,
    snapshot_date,
    fundamentals_asof_date
from {{ ref('fct_financials_snapshot') }}
where fundamentals_asof_date > snapshot_date
