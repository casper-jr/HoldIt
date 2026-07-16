-- ============================================================
-- 11 · Segmentation — turn a measure into a new dimension
-- ------------------------------------------------------------
-- Purpose : Bucket a measure into ranges with CASE, then aggregate
--           over the new segment (value band, valuation percentile,
--           dividend behavior).
-- Reads   : holdit_gold.fct_metrics, holdit_gold.mart_price_history
-- ============================================================

-- Q1. Value segment by PER band -> count and average ROE per band.
WITH seg AS (
  SELECT
    roe,
    CASE
      WHEN per IS NULL OR per <= 0 THEN 'No / negative earnings'
      WHEN per < 8  THEN 'Deep value (<8)'
      WHEN per < 15 THEN 'Value (8-15)'
      WHEN per < 25 THEN 'Fair (15-25)'
      ELSE 'Expensive (>25)'
    END AS per_band
  FROM holdit_gold.fct_metrics
  WHERE snapshot_date = '2026-07-15'
)
SELECT per_band, COUNT(*) AS stocks, ROUND(AVG(roe), 1) AS avg_roe
FROM seg
GROUP BY per_band
ORDER BY stocks DESC;

-- Q2. Cheap-vs-own-history segment (latest row per ticker from mart_price_history).
WITH latest AS (
  SELECT ticker, per_percentile_vs_history,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) AS rn
  FROM holdit_gold.mart_price_history
  WHERE per_percentile_vs_history IS NOT NULL
),
seg AS (
  SELECT CASE
           WHEN per_percentile_vs_history < 0.20 THEN 'Cheap vs self (<20%)'
           WHEN per_percentile_vs_history > 0.80 THEN 'Rich vs self (>80%)'
           ELSE 'Mid range'
         END AS band
  FROM latest WHERE rn = 1
)
SELECT band, COUNT(*) AS stocks
FROM seg
GROUP BY band
ORDER BY stocks DESC;

-- Q3. Dividend behavior segment -> average debt ratio per segment.
WITH seg AS (
  SELECT
    debt_ratio,
    CASE
      WHEN dividend_yield IS NULL THEN 'Non-payer'
      WHEN dividend_yield < 2 THEN 'Payer <2%'
      WHEN dividend_yield < 4 THEN 'Payer 2-4%'
      ELSE 'Payer >4%'
    END AS dividend_segment
  FROM holdit_gold.fct_metrics
  WHERE snapshot_date = '2026-07-15'
)
SELECT dividend_segment, COUNT(*) AS stocks, ROUND(AVG(debt_ratio), 0) AS avg_debt_ratio
FROM seg
GROUP BY dividend_segment
ORDER BY dividend_segment;
