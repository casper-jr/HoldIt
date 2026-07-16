-- ============================================================
-- 04 · Key measures
-- ------------------------------------------------------------
-- Purpose : The headline aggregates for the current snapshot —
--           the "big numbers" that frame everything else.
-- Reads   : holdit_gold.fct_metrics, holdit_gold.mart_leaderboard
-- ============================================================

-- Q1. Universe size (rows vs distinct tickers).
SELECT COUNT(*) AS row_count, COUNT(DISTINCT ticker) AS tickers
FROM holdit_gold.fct_metrics
WHERE snapshot_date = '2026-07-15';

-- Q2. Average valuation/quality metrics (AVG ignores NULLs).
SELECT
  ROUND(AVG(per), 2)            AS avg_per,
  ROUND(AVG(pbr), 2)            AS avg_pbr,
  ROUND(AVG(roe), 2)            AS avg_roe,
  ROUND(AVG(dividend_yield), 2) AS avg_dividend_yield
FROM holdit_gold.fct_metrics
WHERE snapshot_date = '2026-07-15';

-- Q3. Stocks per grade (v2 model).
SELECT grade, COUNT(*) AS n
FROM holdit_gold.mart_leaderboard
WHERE model_version = 'v2' AND snapshot_date = '2026-07-15'
GROUP BY grade
ORDER BY grade;

-- Q4. Single key-metrics report (metric_name, metric_value) via UNION ALL.
SELECT 'total_stocks' AS metric, CAST(COUNT(DISTINCT ticker) AS FLOAT64) AS value
  FROM holdit_gold.fct_metrics WHERE snapshot_date = '2026-07-15'
UNION ALL SELECT 'median_per', APPROX_QUANTILES(per, 2)[OFFSET(1)]
  FROM holdit_gold.fct_metrics WHERE snapshot_date = '2026-07-15'
UNION ALL SELECT 'avg_roe', ROUND(AVG(roe), 2)
  FROM holdit_gold.fct_metrics WHERE snapshot_date = '2026-07-15'
UNION ALL SELECT 'pct_dividend_payers',
  ROUND(SAFE_DIVIDE(COUNTIF(dividend_yield IS NOT NULL), COUNT(*)) * 100, 1)
  FROM holdit_gold.fct_metrics WHERE snapshot_date = '2026-07-15';
