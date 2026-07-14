-- Bronze table definitions. dbt never builds these — Bronze is a source, not a
-- model — so the DDL lives here and the Airflow load_bronze task loads them with
-- partition-overwrite (bq load --replace into the snapshot_date partition).
--
-- Common schema for every raw_{source}_{endpoint} table. payload and
-- request_params are native BigQuery JSON (validated at load, queryable with
-- JSON_VALUE without PARSE_JSON). Partitioned by snapshot_date, clustered by
-- ticker. All columns nullable — missing is NULL, never a default.
--
-- US-first: the four yf tables now; the four raw_dart_* tables land in Step 6.

CREATE TABLE IF NOT EXISTS holdit_bronze.raw_yf_quote (
  snapshot_date  DATE,
  ingested_at    TIMESTAMP,
  source         STRING,
  endpoint       STRING,
  ticker         STRING,
  request_params JSON,
  http_status    INT64,
  payload        JSON
) PARTITION BY snapshot_date CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS holdit_bronze.raw_yf_price_history (
  snapshot_date  DATE,
  ingested_at    TIMESTAMP,
  source         STRING,
  endpoint       STRING,
  ticker         STRING,
  request_params JSON,
  http_status    INT64,
  payload        JSON
) PARTITION BY snapshot_date CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS holdit_bronze.raw_yf_financials (
  snapshot_date  DATE,
  ingested_at    TIMESTAMP,
  source         STRING,
  endpoint       STRING,
  ticker         STRING,
  request_params JSON,
  http_status    INT64,
  payload        JSON
) PARTITION BY snapshot_date CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS holdit_bronze.raw_yf_dividends (
  snapshot_date  DATE,
  ingested_at    TIMESTAMP,
  source         STRING,
  endpoint       STRING,
  ticker         STRING,
  request_params JSON,
  http_status    INT64,
  payload        JSON
) PARTITION BY snapshot_date CLUSTER BY ticker;
