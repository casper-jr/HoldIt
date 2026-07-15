-- Parses raw_yf_price_history: a pandas DataFrame serialized orient='split', so
-- payload = {columns, index, data}. The index carries the trading dates — price_date
-- comes from there, never from snapshot_date. auto_adjust=False fixes the column
-- order to [Open, High, Low, Close, Adj Close, Volume, Dividends, Stock Splits], so
-- OHLCV is read positionally. One output row per (ticker, snapshot_date, price_date).
with source as (
    select ticker, snapshot_date, payload
    from {{ source('bronze', 'raw_yf_price_history') }}
)

select
    s.ticker,
    s.snapshot_date,
    date(timestamp(json_value(json_query_array(s.payload, '$.index')[offset(i)]))) as price_date,
    safe_cast(json_value(json_query_array(row_json)[offset(0)]) as float64) as open,
    safe_cast(json_value(json_query_array(row_json)[offset(1)]) as float64) as high,
    safe_cast(json_value(json_query_array(row_json)[offset(2)]) as float64) as low,
    safe_cast(json_value(json_query_array(row_json)[offset(3)]) as float64) as close,
    safe_cast(json_value(json_query_array(row_json)[offset(4)]) as float64) as adj_close,
    safe_cast(json_value(json_query_array(row_json)[offset(5)]) as int64)   as volume
from source s,
    unnest(json_query_array(s.payload, '$.data')) as row_json with offset i
