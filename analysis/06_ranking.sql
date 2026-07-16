-- ============================================================
-- 06 · Ranking — top / bottom performers
-- ------------------------------------------------------------
-- Purpose : Order stocks by a measure to surface leaders and
--           laggards (LIMIT and window-function approaches).
-- Reads   : holdit_gold.fct_metrics + holdit_silver.dim_company,
--           holdit_gold.mart_leaderboard
-- ============================================================

-- Q1. Top 10 stocks by ROE.
SELECT m.ticker, c.name, c.sector, ROUND(m.roe, 1) AS roe
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15' AND m.roe IS NOT NULL
ORDER BY m.roe DESC
LIMIT 10;

-- Q2. Cheapest 10 profitable stocks by PER.
SELECT m.ticker, c.name, c.sector, ROUND(m.per, 1) AS per
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15' AND m.per > 0
ORDER BY m.per ASC
LIMIT 10;

-- Q3. Top 10 by v2 normalized score — the actual screen result.
SELECT rank, ticker, sector, normalized_score, grade
FROM holdit_gold.mart_leaderboard
WHERE model_version = 'v2' AND snapshot_date = '2026-07-15'
ORDER BY rank
LIMIT 10;

-- Q4. Same as Q1 with a window function (more flexible than LIMIT).
SELECT ticker, name, sector, roe
FROM (
  SELECT m.ticker, c.name, c.sector, ROUND(m.roe, 1) AS roe,
         ROW_NUMBER() OVER (ORDER BY m.roe DESC) AS rn
  FROM holdit_gold.fct_metrics m
  JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
  WHERE m.snapshot_date = '2026-07-15' AND m.roe IS NOT NULL
)
WHERE rn <= 10
ORDER BY rn;
