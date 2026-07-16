-- Parses raw_yf_quote: the yfinance info dict, flat JSON. Metadata only — the
-- price fields are exposed but deliberately unused downstream (fct_price_daily is
-- the only price source). Missing keys parse to NULL; nothing is defaulted.
with source as (
    select ticker, snapshot_date, payload
    from {{ source('bronze', 'raw_yf_quote') }}
)

select
    ticker,
    snapshot_date,
    json_value(payload, '$.longName')          as long_name,
    json_value(payload, '$.shortName')          as short_name,
    json_value(payload, '$.exchange')           as exchange_code,
    json_value(payload, '$.quoteType')          as quote_type,
    json_value(payload, '$.sector')             as sector,
    json_value(payload, '$.industry')           as industry,
    json_value(payload, '$.currency')           as currency,
    json_value(payload, '$.financialCurrency')  as financial_currency,
    safe_cast(json_value(payload, '$.sharesOutstanding') as int64)  as shares_outstanding,
    safe_cast(json_value(payload, '$.marketCap')         as int64)  as market_cap,
    -- price fields kept for completeness; quote price is not the price source
    safe_cast(json_value(payload, '$.currentPrice')  as float64)    as current_price,
    safe_cast(json_value(payload, '$.previousClose') as float64)    as previous_close,
    -- Yahoo's own currency-consistent PER, used in fct_metrics to recover a valid PER
    -- for ADRs whose price currency differs from their financial currency.
    safe_cast(json_value(payload, '$.trailingPE')    as float64)    as trailing_pe
from source
