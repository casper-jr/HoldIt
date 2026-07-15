-- Daily price + valuation series per ticker with the trailing analytics the Analysis
-- Questions need: drawdown from the 1-year high (Q6, exact — prices are observed), the
-- PER percentile against the ticker's own history (Q5, pre-launch portion reconstructed
-- so is_reconstructed rides along), and 1-year realized volatility. Reads
-- fct_valuation_daily. Partitioned by price_date.
{{ config(partition_by={'field': 'price_date', 'data_type': 'date'}) }}

with returns as (
    select
        ticker,
        price_date,
        close,
        per,
        pbr,
        is_reconstructed,
        close / nullif(lag(close) over (partition by ticker order by price_date), 0) - 1 as daily_return
    from {{ ref('fct_valuation_daily') }}
)

select
    ticker,
    price_date,
    close,
    per,
    pbr,
    is_reconstructed,
    max(close) over w_1yr as high_1yr,
    round(close / nullif(max(close) over w_1yr, 0) - 1, 4) as drawdown_from_1yr_high,
    round(percent_rank() over (partition by ticker order by per), 3) as per_percentile_vs_history,
    round(stddev_samp(daily_return) over w_1yr * sqrt(252), 4) as volatility_1yr
from returns
window w_1yr as (
    partition by ticker order by price_date
    rows between 251 preceding and current row
)
