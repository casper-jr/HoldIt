-- ============================================================
-- 03 · Time span
-- ------------------------------------------------------------
-- Purpose : Establish the data's temporal coverage — price history
--           depth and snapshot cadence.
-- Reads   : holdit_silver.fct_price_daily, holdit_gold.fct_metrics
-- ============================================================

-- Q1. Earliest and latest price date.
SELECT MIN(price_date) AS earliest, MAX(price_date) AS latest
FROM holdit_silver.fct_price_daily;

-- Q2. How many years that span covers.
SELECT DATE_DIFF(MAX(price_date), MIN(price_date), YEAR) AS years_of_history
FROM holdit_silver.fct_price_daily;

-- Q3. Live metric snapshots and their coverage.
SELECT snapshot_date, COUNT(*) AS tickers
FROM holdit_gold.fct_metrics
GROUP BY snapshot_date
ORDER BY snapshot_date;
