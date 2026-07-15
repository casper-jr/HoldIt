-- Parses raw_yf_dividends: a pandas Series serialized orient='split', so payload =
-- {name, index, data} where index = ex-dividend dates and data = cash amounts. One
-- row per individual dividend event; fct_dividend_history aggregates to fiscal year.
with source as (
    select ticker, snapshot_date, payload
    from {{ source('bronze', 'raw_yf_dividends') }}
)

select
    s.ticker,
    s.snapshot_date,
    date(timestamp(json_value(json_query_array(s.payload, '$.index')[offset(i)]))) as ex_date,
    safe_cast(json_value(amount) as float64) as dividend
from source s,
    unnest(json_query_array(s.payload, '$.data')) as amount with offset i
