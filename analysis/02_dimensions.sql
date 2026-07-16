-- ============================================================
-- 02 · Dimensions
-- ------------------------------------------------------------
-- Purpose : Profile the dimensions — unique values and cardinality
--           that shape every later GROUP BY.
-- Reads   : holdit_silver.dim_company
-- ============================================================

-- Q1. Companies per market.
SELECT market, COUNT(*) AS companies
FROM holdit_silver.dim_company
WHERE is_current
GROUP BY market
ORDER BY companies DESC;

-- Q2. Companies per sector (cardinality predicts row counts when you group by sector later).
SELECT sector, COUNT(*) AS companies
FROM holdit_silver.dim_company
WHERE is_current
GROUP BY sector
ORDER BY companies DESC;

-- Q3. The sector -> industry hierarchy: sub-industries per sector.
SELECT sector, COUNT(DISTINCT industry) AS industries
FROM holdit_silver.dim_company
WHERE is_current
GROUP BY sector
ORDER BY industries DESC;
