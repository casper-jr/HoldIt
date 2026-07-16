# analysis/ — SQL analysis on the Gold layer

The analytical layer of the warehouse: exploratory and advanced-analytics SQL written
against the Gold and Silver models, plus two reusable reporting views. These scripts
answer business questions directly on **BigQuery** and feed the Tableau dashboard.

Each file is a self-contained set of queries with a header stating its purpose and the
tables it reads. The progression runs from orientation (what's in the warehouse) through
single-question analyses to two `CREATE VIEW` capstones a BI tool can connect to
directly — `holdit_gold.report_stock` and `holdit_gold.report_sector`.

## How to run
Open the BigQuery console with the project selected and run a file's statements, or use
the CLI: `bq query --nouse_legacy_sql 'SELECT ...'`. With the project set you can write
`holdit_gold.fct_metrics`; otherwise prefix the project id.

## Contents
```
Exploration   01 database_exploration   02 dimensions            03 time_span
              04 key_measures           05 magnitude             06 ranking
Advanced      07 change_over_time       08 cumulative            09 performance
              10 part_to_whole          11 segmentation
Reports       12 report_stock           13 report_sector         (CREATE VIEW)
```

## Schema reference

**holdit_silver**
- `dim_company` — `ticker, name, market, sector, industry, currency, is_delisted, valid_from, valid_to, is_current`
- `fct_price_daily` — `ticker, price_date, open, high, low, close, adj_close, volume, is_backfilled`
- `fct_financials_snapshot` — `ticker, snapshot_date, is_reconstructed, net_income, total_revenue, shares_outstanding, stockholders_equity, total_debt, total_assets, total_liabilities, free_cash_flow, income_period_end, fundamentals_asof_date`
- `fct_dividend_history` — `ticker, fiscal_year, dividend_per_share, num_payments`

**holdit_gold**
- `fct_metrics` — `ticker, snapshot_date, close, per, pbr, roe, fcf_yield, debt_ratio, dividend_yield, peg, div_growth_years`
- `fct_valuation_daily` — `ticker, price_date, close, is_reconstructed, per, pbr, fcf_yield`
- `mart_leaderboard` — `ticker, snapshot_date, model_version, name, sector, total_score, normalized_score, grade, rank`
- `mart_model_comparison` — `ticker, v1_rank, v2_rank, rank_delta, rank_corr_spearman`
- `mart_sector_valuation` — `sector, per, sector_per_median, sector_per_median_3yr, per_vs_sector_3yr, includes_reconstructed`
- `mart_price_history` — `ticker, price_date, per, drawdown_from_1yr_high, per_percentile_vs_history, volatility_1yr`
- `mart_data_quality` — `snapshot_date, metric, ticker_count, null_count, null_rate`

The current live snapshot for metrics/scores is **`2026-07-15`** (954 US tickers). Daily
price/valuation spans ~5 years.
