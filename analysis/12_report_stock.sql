-- ============================================================
-- 12 · Report view: report_stock
-- ------------------------------------------------------------
-- Purpose : A reusable per-stock reporting view — identity, valuation,
--           price signal, score, and derived segments in one place, so
--           a BI tool reads one view instead of joining facts itself.
--           Built with a base -> join -> final CTE pattern.
-- Reads   : fct_metrics, dim_company, mart_price_history, mart_leaderboard
-- Creates : holdit_gold.report_stock
-- ============================================================

CREATE OR REPLACE VIEW holdit_gold.report_stock AS
WITH base AS (
  SELECT
    m.ticker, c.name, c.sector, c.market,
    m.close, m.per, m.pbr, m.roe, m.dividend_yield, m.peg, m.debt_ratio, m.div_growth_years
  FROM holdit_gold.fct_metrics m
  JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
  WHERE m.snapshot_date = (SELECT MAX(snapshot_date) FROM holdit_gold.fct_metrics)
),
price AS (
  SELECT ticker, drawdown_from_1yr_high, per_percentile_vs_history, volatility_1yr
  FROM (
    SELECT ticker, drawdown_from_1yr_high, per_percentile_vs_history, volatility_1yr,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) AS rn
    FROM holdit_gold.mart_price_history
  )
  WHERE rn = 1
),
score AS (
  SELECT ticker, normalized_score, grade, rank
  FROM holdit_gold.mart_leaderboard
  WHERE model_version = 'v2'
    AND snapshot_date = (SELECT MAX(snapshot_date) FROM holdit_gold.mart_leaderboard)
)
SELECT
  b.ticker, b.name, b.sector, b.market,
  b.close, b.per, b.pbr, b.roe, b.dividend_yield, b.peg, b.debt_ratio, b.div_growth_years,
  p.drawdown_from_1yr_high, p.per_percentile_vs_history, p.volatility_1yr,
  s.normalized_score, s.grade, s.rank,
  CASE
    WHEN b.per IS NULL OR b.per <= 0 THEN 'No / negative earnings'
    WHEN b.per < 8  THEN 'Deep value'
    WHEN b.per < 15 THEN 'Value'
    WHEN b.per < 25 THEN 'Fair'
    ELSE 'Expensive'
  END AS value_segment,
  (b.roe > 15 AND b.debt_ratio < 60) AS is_quality
FROM base b
LEFT JOIN price p USING (ticker)
LEFT JOIN score s USING (ticker);

-- Sanity check after creating it:
-- SELECT * FROM holdit_gold.report_stock ORDER BY rank LIMIT 20;
