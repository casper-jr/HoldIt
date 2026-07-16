-- ============================================================
-- 07 · Change over time — trend and seasonality
-- ------------------------------------------------------------
-- Purpose : Track a measure across time to read trend and re-rating.
--           is_reconstructed = TRUE for pre-launch (estimated) rows.
-- Reads   : holdit_gold.fct_valuation_daily + holdit_silver.dim_company
-- ============================================================

-- Q1. One stock's average PER by year (flag years that include reconstructed data).
SELECT
  EXTRACT(YEAR FROM price_date) AS yr,
  ROUND(AVG(per), 2) AS avg_per,
  COUNT(*) AS trading_days,
  LOGICAL_OR(is_reconstructed) AS any_reconstructed
FROM holdit_gold.fct_valuation_daily
WHERE ticker = 'AAPL' AND per IS NOT NULL
GROUP BY yr
ORDER BY yr;

-- Q2. One stock's average PER by month (DATE_TRUNC keeps it sortable as a real date).
SELECT
  DATE_TRUNC(price_date, MONTH) AS month,
  ROUND(AVG(per), 2) AS avg_per
FROM holdit_gold.fct_valuation_daily
WHERE ticker = 'AAPL' AND per IS NOT NULL
GROUP BY month
ORDER BY month;

-- Q3. Sector median PER by year — is a sector re-rating over time?
SELECT
  c.sector,
  EXTRACT(YEAR FROM v.price_date) AS yr,
  APPROX_QUANTILES(v.per, 2)[OFFSET(1)] AS median_per
FROM holdit_gold.fct_valuation_daily v
JOIN holdit_silver.dim_company c ON c.ticker = v.ticker AND c.is_current
WHERE v.per IS NOT NULL
GROUP BY c.sector, yr
ORDER BY c.sector, yr;
