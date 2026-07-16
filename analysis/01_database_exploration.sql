-- ============================================================
-- 01 · Database exploration
-- ------------------------------------------------------------
-- Purpose : Inventory the warehouse before analysis — enumerate
--           tables and inspect column schemas.
-- Reads   : holdit_gold, holdit_silver (INFORMATION_SCHEMA)
-- ============================================================

-- Q1. Every table/view in the Gold dataset.
SELECT table_name, table_type
FROM `holdit_gold`.INFORMATION_SCHEMA.TABLES
ORDER BY table_name;

-- Q2. Columns of fct_metrics (name + type).
SELECT column_name, data_type
FROM `holdit_gold`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'fct_metrics'
ORDER BY ordinal_position;

-- Q3. How many objects each layer exposes.
SELECT 'silver' AS layer, COUNT(*) AS objects FROM `holdit_silver`.INFORMATION_SCHEMA.TABLES
UNION ALL
SELECT 'gold', COUNT(*) FROM `holdit_gold`.INFORMATION_SCHEMA.TABLES;
