-- ============================================================
-- 08 · Cumulative — running totals and moving averages
-- ------------------------------------------------------------
-- Purpose : Aggregate progressively over time with window frames
--           (moving average, year-to-date, running high).
-- Reads   : holdit_gold.fct_valuation_daily, holdit_silver.fct_price_daily
-- ============================================================

-- Q1. 90-trading-day moving-average PER for one stock.
SELECT
  ticker, price_date, ROUND(per, 2) AS per,
  ROUND(AVG(per) OVER (
    PARTITION BY ticker ORDER BY price_date
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  ), 2) AS per_ma90
FROM holdit_gold.fct_valuation_daily
WHERE ticker = 'AAPL' AND per IS NOT NULL
ORDER BY price_date;

-- Q2. Cumulative (year-to-date) average PER, reset each calendar year.
SELECT
  ticker, price_date, ROUND(per, 2) AS per,
  ROUND(AVG(per) OVER (
    PARTITION BY ticker, EXTRACT(YEAR FROM price_date) ORDER BY price_date
  ), 2) AS ytd_avg_per
FROM holdit_gold.fct_valuation_daily
WHERE ticker = 'AAPL' AND per IS NOT NULL
ORDER BY price_date;

-- Q3. Running all-time-high close (the basis for a drawdown).
SELECT
  ticker, price_date, close,
  MAX(close) OVER (
    PARTITION BY ticker ORDER BY price_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_high
FROM holdit_silver.fct_price_daily
WHERE ticker = 'AAPL'
ORDER BY price_date;
