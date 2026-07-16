-- ============================================================
-- 10 · Part-to-whole — a part's share of the total
-- ------------------------------------------------------------
-- Purpose : Express each group as a percentage of the whole, using
--           a windowed grand total (SUM(...) OVER ()).
-- Reads   : holdit_gold.fct_metrics + holdit_silver.dim_company,
--           holdit_gold.mart_leaderboard
-- ============================================================

-- Q1. Each sector's share of the universe by stock count.
SELECT
  c.sector,
  COUNT(*) AS stocks,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) * 100, 1) AS pct_of_universe
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15'
GROUP BY c.sector
ORDER BY stocks DESC;

-- Q2. Grade distribution for v2 (share of each grade).
SELECT
  grade,
  COUNT(*) AS n,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) * 100, 1) AS pct
FROM holdit_gold.mart_leaderboard
WHERE model_version = 'v2' AND snapshot_date = '2026-07-15'
GROUP BY grade
ORDER BY grade;

-- Q3. Which sectors hold the top-50 screened stocks (v2), and their share of that top 50.
WITH top50 AS (
  SELECT sector
  FROM holdit_gold.mart_leaderboard
  WHERE model_version = 'v2' AND snapshot_date = '2026-07-15' AND rank <= 50
)
SELECT
  sector,
  COUNT(*) AS in_top_50,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) * 100, 1) AS pct_of_top_50
FROM top50
GROUP BY sector
ORDER BY in_top_50 DESC;
