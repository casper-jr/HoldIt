-- Parses raw_yf_financials: three DataFrames (income_stmt, balance_sheet, cashflow),
-- each serialized orient='split' as {columns, index, data}. columns = period-end
-- dates, index = line-item labels, data[i][j] = value of line-item i at period j.
-- Unpivots each matrix to long form so the core model picks line-items by name
-- rather than by fragile column position. One row per cell.
{% set statements = ['income_stmt', 'balance_sheet', 'cashflow'] %}

with source as (
    select ticker, snapshot_date, payload
    from {{ source('bronze', 'raw_yf_financials') }}
)

{% for stmt in statements %}
select
    s.ticker,
    s.snapshot_date,
    '{{ stmt }}' as statement,
    json_value(json_query_array(s.payload, '$.{{ stmt }}.index')[offset(i)])    as line_item,
    date(timestamp(json_value(json_query_array(s.payload, '$.{{ stmt }}.columns')[offset(j)]))) as period_end,
    safe_cast(json_value(cell) as float64) as value
from source s,
    unnest(json_query_array(s.payload, '$.{{ stmt }}.data')) as row_json with offset i,
    unnest(json_query_array(row_json)) as cell with offset j
{% if not loop.last %}union all{% endif %}
{% endfor %}
