-- ============================================================
-- 13 · Report view: report_sector
-- ------------------------------------------------------------
-- Purpose : A sector-level reporting view — medians, dividend-payer
--           share, universe weight, and a cheap-vs-own-history flag.
--           includes_reconstructed carries through, so any 3-year
--           comparison stays honest about estimated pre-launch history.
-- Reads   : fct_metrics, dim_company, mart_sector_valuation
-- Creates : holdit_gold.report_sector
-- ============================================================

CREATE OR REPLACE VIEW holdit_gold.report_sector AS
WITH base AS (
  SELECT c.sector, m.per, m.pbr, m.roe, m.dividend_yield
  FROM holdit_gold.fct_metrics m
  JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
  WHERE m.snapshot_date = (SELECT MAX(snapshot_date) FROM holdit_gold.fct_metrics)
),
agg AS (
  SELECT
    sector,
    COUNT(*) AS stock_count,
    APPROX_QUANTILES(per, 2)[OFFSET(1)] AS median_per,
    APPROX_QUANTILES(pbr, 2)[OFFSET(1)] AS median_pbr,
    APPROX_QUANTILES(roe, 2)[OFFSET(1)] AS median_roe,
    ROUND(SAFE_DIVIDE(COUNTIF(dividend_yield IS NOT NULL), COUNT(*)) * 100, 1) AS pct_dividend_payers
  FROM base
  GROUP BY sector
),
hist AS (
  SELECT
    sector,
    ANY_VALUE(sector_per_median_3yr) AS median_per_3yr,
    LOGICAL_OR(includes_reconstructed) AS includes_reconstructed
  FROM holdit_gold.mart_sector_valuation
  GROUP BY sector
)
SELECT
  a.sector,
  a.stock_count,
  ROUND(SAFE_DIVIDE(a.stock_count, SUM(a.stock_count) OVER ()) * 100, 1) AS pct_of_universe,
  a.median_per,
  a.median_pbr,
  a.median_roe,
  a.pct_dividend_payers,
  h.median_per_3yr,
  h.includes_reconstructed,
  (a.median_per < h.median_per_3yr) AS cheap_vs_own_history
FROM agg a
LEFT JOIN hist h USING (sector)
ORDER BY a.stock_count DESC;

-- Sanity check after creating it:
-- SELECT * FROM holdit_gold.report_sector;
