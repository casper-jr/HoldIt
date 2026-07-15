-- No metric may be dated in the future — a snapshot_date past today means a date bug in
-- ingestion or the spine, not real data.
select ticker, snapshot_date
from {{ ref('fct_metrics') }}
where snapshot_date > current_date()
