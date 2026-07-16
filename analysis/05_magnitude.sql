-- ============================================================
-- 05 · Magnitude — a measure broken down by a dimension
-- ------------------------------------------------------------
-- Purpose : Aggregate a measure by dimension (sector, market) to
--           see where the universe concentrates.
-- Reads   : holdit_gold.fct_metrics + holdit_silver.dim_company
-- ============================================================

-- Q1. Median PER by sector (cheapest sectors first).
SELECT
  c.sector,
  APPROX_QUANTILES(m.per, 2)[OFFSET(1)] AS median_per,
  COUNT(*) AS stocks
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15' AND m.per IS NOT NULL
GROUP BY c.sector
ORDER BY median_per;

-- Q2. Average ROE by sector (most profitable first).
SELECT c.sector, ROUND(AVG(m.roe), 1) AS avg_roe
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15'
GROUP BY c.sector
ORDER BY avg_roe DESC;

-- Q3. Count and average dividend yield by market.
SELECT c.market, COUNT(*) AS stocks, ROUND(AVG(m.dividend_yield), 2) AS avg_dividend_yield
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15'
GROUP BY c.market
ORDER BY stocks DESC;
