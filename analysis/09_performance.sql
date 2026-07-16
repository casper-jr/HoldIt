-- ============================================================
-- 09 · Performance — current vs a benchmark
-- ------------------------------------------------------------
-- Purpose : Measure variance against a target — prior period (YoY),
--           the stock's own average, and its sector average.
-- Reads   : holdit_silver.fct_financials_snapshot,
--           holdit_gold.fct_valuation_daily, holdit_gold.fct_metrics
-- ============================================================

-- Q1. Year-over-year net income for one stock (increase / decrease flag).
WITH annual AS (
  SELECT DISTINCT ticker, income_period_end, net_income
  FROM holdit_silver.fct_financials_snapshot
  WHERE income_period_end IS NOT NULL AND net_income IS NOT NULL
)
SELECT
  ticker, income_period_end, net_income,
  LAG(net_income) OVER (PARTITION BY ticker ORDER BY income_period_end) AS prev_net_income,
  net_income - LAG(net_income) OVER (PARTITION BY ticker ORDER BY income_period_end) AS yoy_change,
  CASE
    WHEN net_income > LAG(net_income) OVER (PARTITION BY ticker ORDER BY income_period_end) THEN 'increase'
    WHEN net_income < LAG(net_income) OVER (PARTITION BY ticker ORDER BY income_period_end) THEN 'decrease'
    ELSE 'no change'
  END AS yoy_flag
FROM annual
WHERE ticker = 'AAPL'
ORDER BY income_period_end;

-- Q2. Each stock's latest PER vs its own historical average PER.
WITH v AS (
  SELECT ticker, per, price_date,
         AVG(per) OVER (PARTITION BY ticker) AS avg_per,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) AS rn
  FROM holdit_gold.fct_valuation_daily
  WHERE per IS NOT NULL
)
SELECT
  ticker, ROUND(per, 2) AS latest_per, ROUND(avg_per, 2) AS own_avg_per,
  CASE WHEN per > avg_per THEN 'above own avg'
       WHEN per < avg_per THEN 'below own avg' ELSE 'at avg' END AS flag
FROM v
WHERE rn = 1
ORDER BY ticker
LIMIT 50;

-- Q3. Each stock's PER vs its sector average (this snapshot).
SELECT
  m.ticker, c.sector, ROUND(m.per, 2) AS per,
  ROUND(AVG(m.per) OVER (PARTITION BY c.sector), 2) AS sector_avg_per,
  CASE WHEN m.per > AVG(m.per) OVER (PARTITION BY c.sector)
       THEN 'above sector' ELSE 'below sector' END AS flag
FROM holdit_gold.fct_metrics m
JOIN holdit_silver.dim_company c ON c.ticker = m.ticker AND c.is_current
WHERE m.snapshot_date = '2026-07-15' AND m.per > 0
ORDER BY c.sector, per;
