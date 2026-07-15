# Refactoring Plan — the worklist

> The mutable doc. Check steps off as they land; correct a step in place if reality disagrees
> with it. For the spec, see `architecture.md`. For why, see `as-is.md`.
>
> **Step 0 is done** — it is the decision this document records, not work remaining.
> **Step 1 is decided but not executed** : the delete list below is settled, the files are still
> there. Do not re-litigate what survives; just carry it out.

## Status

- [x] Step 0 — Clarify the goal
- [x] Step 1 — Decide what survives
- [x] Step 2 — Reset GCP and stand up the skeleton
- [x] Step 3 — Build Bronze and the ingestion path (proven end-to-end; 89-ticker bulk seed loaded)
- [x] Step 4 — Build Silver (8 models + snapshot, all tests green on prod; DAG gate wired)
- [x] Step 5 — Build Gold (metrics + scoring + 6 marts; v1/v2 both scored; 77/77 prod build)
- [~] Step 5.5 — Scale US to the full universe (in progress)
- [ ] Step 8 — Rewrite `README.md` (with the full-scale data-quality baseline)
- [ ] Step 7 — Create the Tableau dashboard (**user-owned** — done by the user, for interview prep, not the assistant)
- [ ] Step 6 — Add KR as the second source (**conditional / deferred** — see decision below)

> **Sequencing decision (2026-07-15).** US is taken all the way to a finished, presentable
> state before any KR work, and the analysis is split by owner:
> - **Assistant**: scale US to the full universe (5.5) → rewrite `README.md` (8) → then
>   two hand-off guides — (a) what to explore in the GCP/BigQuery console with SQL at each
>   layer, and (b) what to focus on in Tableau for maximum resume/interview impact.
> - **User**: builds the Tableau dashboard and does the analysis themselves (Step 7) —
>   deliberately, because owning the analysis is what an interview tests.
> - **Step 6 (KR) is conditional, not committed**: it starts with a Bronze DART null-rate
>   probe, and if coverage is poor the KR analysis may be dropped rather than forced.
>   `cancel` scoring and KR moat rows stay parked until then.
>
> Step numbers are kept for continuity; execution order is 5 → 5.5 → 8 → (user: 7) →
> (maybe) 6.

---

### Step 0 — Clarify the goal
- Refactoring a project to make the scoring and screening process more efficient and easy to use by creating the Data Warehouse and 3 layers
- Implement a full End to End(Data Warehouse -> Data Analytics)
- This is not for a product-level project for real-business. I will use this as a personal project and a porfolio for my resume. Using GCP is for the Cloud-Environment requirements on the job postings, but I will use free tools or APIs for other things such as data source and tools if possible

### Step 1 — Decide what survives
- Reduce Python to ingestion only. Once Silver and Gold move to dbt, the rest is **deleted, not reorganized**
- Delete : `backtest.py`, `bq_analytics.py`, `processor.py`, `scorers/`, `main.py`, `models.py`, `database.py`, `gcs_upload.py`, `reset_db.py`, `test_db.py`, `entrypoint.sh`, `.github/workflows/weekly_export.yml`, `export/`
- Rewrite : `fetcher.py` → the `ingestion/` package. It writes raw payloads and derives nothing. Its `main.py` is a new file, not the old god-module
- The scoring logic is not "moved to human" — it stays fully automated, in SQL. Only the **moat** score is human, and it enters through `seed_qualitative_moat.csv`

**Data Source**
- Build **US first** with yfinance. The last 3 weekly exports contain 246 US vs 15 KR stocks above the threshold, so KR is measurably the broken one
- Add KR second, after the US path works end to end. Conforming two sources into one Silver model is a better demonstration than doing one source twice, and it is why `dim_company` and `currency` are designed for it from the start
- Before committing to KR, measure DART null rates **in Bronze** rather than guessing. If coverage stays poor, fall back to FinanceDataReader or KRX for fundamentals

### Step 2 — Reset GCP and stand up the skeleton
- Delete the Cloud SQL instance, the Cloud Scheduler job, and the existing `holdit.stock_scores` table
- Create the three datasets, the two dev datasets, and `gs://holdit-raw/`
- Create the dbt project with `dev` and `prod` targets
- Stand up Airflow in Docker Compose with the service accounts, and get one trivial DAG green before writing any real task

### Step 3 — Build Bronze and the ingestion path
- Ingestion fetches and writes the untouched payload to GCS, clearing the `{ds}` prefix first, deriving nothing
- Load GCS → Bronze with partition overwrite
- **The bulk backfill is a one-time manual load, not `airflow dags backfill`.** 5 years × 52 weeks × 1200 tickers would be ~312k yfinance calls and immediate rate-limiting. Instead : one `history(start, end)` call per ticker for the whole 5-year span, plus one annual-statements call per ticker. ~2400 calls total, one Bronze row each, `snapshot_date` = the request as-of date, `request_params` bounding the range
- These two backfills are separate things and must not be conflated : the **one-time bulk load** seeds history, while **`airflow dags backfill`** recovers a missed weekly run. Only the second is `{{ ds }}`-driven
- **Exit criteria** : run the same date twice, assert the row count does not change. Wire `ingest_us` → `load_bronze` with `{{ ds }}`, and run `airflow dags backfill` over 3 historical dates to prove the weekly path is date-parameterized
- **Progress (2026-07-15)** — exit criteria met on a 20-ticker US slice for `quote` + `price_history`:
	- Bronze DDL lives in `bronze/create_bronze_tables.sql` (the tree had no home for it). The four `raw_yf_*` tables exist, partitioned by `snapshot_date`, clustered by `ticker`
	- Ingestion → GCS → `bq load --replace` per partition proven; re-loading a date leaves the count unchanged (idempotent); payload is native JSON, `http_status` NULL for yf, both `Close`/`Adj Close` kept, and the `price_history` index carries dates distinct from `snapshot_date`
	- DAG `holdit_weekly` (`airflow/dags/holdit_weekly.py`) wires `ingest_us_* → load_bronze_*`; `airflow dags backfill` over 06-19/06-26/07-03 produced three distinct partitions with distinct payload windows
	- **Deviation from the spec, deliberate & temporary** : ingestion runs *in the Airflow container* via `BashOperator` (image `airflow/Dockerfile`, host ADC mounted), not yet a Cloud Run Job via `CloudRunExecuteJobOperator`. Migrate before Step 6 — the Q1 decision to defer Cloud Run plumbing until the logic is proven
	- **Remaining before Step 4** : add `financials` + `dividends` to the DAG (same path), and run the one-time 5-year bulk backfill over the full universe (the slice does not seed enough history for the Silver reconstruction)

### Step 4 — Build Silver
- `dim_company` as a dbt snapshot, `fct_price_daily` at (`ticker`, `price_date`), `fct_financials_snapshot` at (`ticker`, `snapshot_date`), `fct_dividend_history` at (`ticker`, `fiscal_year`)
- `fct_price_daily` first — it is the only price source, and `fct_financials_snapshot` carries no price column. Derive `price_date` from the payload index, never from `snapshot_date`
- Then the `fct_financials_snapshot` reconstruction : synthesize the pre-launch weekly grid from annual filings, apply the filing lag (real `rcept_dt` for KR, +90d for US), and set `is_reconstructed` / `fundamentals_asof_date`
- Resolve ADR currency mismatch in SQL, removing the last API call from the transform path
- Write the dbt tests **before** the models, and add `dbt_test_silver` to the DAG as a hard gate
- **Exit criteria** — note that PER does not exist yet, it is Gold in Step 5, so nothing here tests it :
	- `fct_price_daily` holds ~250 rows per ticker per backfilled year, and one ticker's `close` differs across 3 consecutive snapshot dates
	- `fct_financials_snapshot` holds 3 distinct rows for one ticker across 3 snapshots. If they are identical, the time dimension does not exist yet
	- No reconstructed row has `fundamentals_asof_date > snapshot_date`. That test failing means lookahead bias is in the data
- **Progress (2026-07-15)** — Step 4 complete, all exit criteria met on prod (`holdit_silver`):
	- 4 `stg_yf__*` staging models (parse-only; financials unpivots the orient='split' matrix to long form). 4 core models + `snap_company`/`dim_company` SCD2. 34 tests pass, incl. `assert_no_lookahead` and `assert_reconstruction_is_labelled`
	- `fct_price_daily` (107.1k rows): AAPL spans 2021→2026 (~250/ticker/yr) with `price_date` from the payload index, `is_backfilled` splitting bulk vs live
	- `fct_financials_snapshot` (23.3k rows): AAPL shows FY22/FY23/FY24 net income across three reconstructed Fridays, then the live FY25 row (`is_reconstructed=false`) — the time dimension. Filing lag = period_end + 90d (US), a `us_filing_lag_days` var; boundary is the `launch_date` var (`2026-07-14`)
	- **Deviations, deliberate** : Silver materializes to `table` not `incremental` (premature with one snapshot — a config change later, not a rewrite); `dim_company` SCD2 accrues forward, not retro from Bronze partitions; `dividend_fiscal_year` dropped from `fct_financials_snapshot` (it duplicates `fct_dividend_history`). All three recorded in `architecture.md`
	- DAG gate `dbt_run_silver → dbt_test_silver` wired in `holdit_weekly.py` (runs `--target prod` via mounted host ADC); `dbt-bigquery` added to the image + dbt project mounted. Proven from host via the identical commands; in-container run needs `docker compose build` (image rebuild)
	- **Remaining before Step 5** : rebuild the Airflow image to run the gate in-container; drop the stray `holdit.seed_exchange_mapping` left in the legacy base dataset by the first seed run

### Step 5 — Build Gold
- `fct_metrics` (weekly) → `int_metrics_unpivot` → seeds → `fct_metric_scores` via the range join
- `fct_valuation_daily` separately, off `fct_price_daily`. It is not scored
- **Exit criteria** : reproduce v2's existing scores from the seed CSV, proving the SQL matches the Python it replaces. Then delete `scorers/`
- Add v1 as **seed rows only, zero code change** — the proof that the model-comparison problem is solved
- Build the six `mart_*` models
- Fix the real scoring bugs while translating : the moat is 10 structurally-zero points (now fed by the seed), and grade thresholds (A > 80) assume a 100-point scale the model never reaches
- **Progress (2026-07-15)** — Step 5 complete, whole warehouse builds 77/77 on prod:
	- `fct_metrics` (live-only, 109 rows) via `safe_divide`; PEG from the annual EPS series and `div_growth_years` from `fct_dividend_history` — both computed where the As-Is left them null. `int_metrics_unpivot` (817 rows) long form for one range join
	- `fct_metric_scores` (923 rows): range join to `seed_scoring_rules` (43 bands, v1+v2). Hand-verified AAPL — v1 total 12, v2 total 42, incl. the v2 PBR-ROE bonus giving pbr 3 despite pbr 63.7. `fct_valuation_daily` (106.9k) as-of join proven lookahead-free (AAPL PER stays NULL until FY22 files 2022-12-29). `assert_scores_are_live_only` passes
	- Six marts. `mart_leaderboard` normalizes to 0-100 (v1=47, v2=100 max) and grades on that — the A>80 bug fixed. `mart_model_comparison`: v1↔v2 Spearman 0.509, BABA #2 (v1) vs #64 (v2). Sector/price-history/data-quality marts shaped for the Analysis Questions
	- **Deviations, deliberate** (in `architecture.md`): (1) `scorers/` was already deleted in Step 1, and `stock_scores` in Step 2, so "reproduce v2's scores" is validated by faithful threshold encoding + hand-checks, not a row diff against deleted data; (2) v2's PBR-ROE bonus is a SQL `CASE`, the one rule not expressible as a pure seed row; (3) `cancel` (share-cancellation) is DART-only → 0 for US now, returns Step 6; (4) `div_growth_years` is approximate (calendar-year dividend aggregation); (5) `dividend_yield` deferred from `fct_valuation_daily` (weekly-only); (6) skipped `assert_per_within_range` / `assert_debt_ratio_non_negative` — both encode false invariants (negative equity legitimately yields negative debt_ratio)
	- DAG extended `dbt_run_gold → dbt_test_gold`; seeds reloaded in `dbt_run_gold`
	- **Data-state note**: the latest snapshot in-warehouse is the 2026-07-15 20-ticker financials proof slice, so "current" marts (`mart_sector_valuation`) use 20 tickers; the 89-ticker set is 2026-07-14. A full weekly run makes latest complete

### Step 6 — Add KR as the second source
- Repeat Step 3 for the DART endpoints, then conform into the **existing** Silver models
- KR is not DART-only : prices for `.KS` / `.KQ` tickers come from yfinance into the same `raw_yf_price_history`, so the KR price path is the US path with different tickers. DART supplies fundamentals, dividends, share counts, and disclosures
- KR reconstruction is the better half of the two : DART gives real `rcept_dt` filing dates, so KR needs no 90-day assumption and its lookahead control is exact
- **Exit criteria** : no Gold model needed changing. If one did, the Silver layer was not conformed properly

### Step 7 — Create the Tableau dashboard
- Connect to `holdit_gold`, build against the Analysis Questions
- Add `refresh_tableau` as the final DAG task

### Step 8 — Rewrite `README.md`
- Architecture diagram, layer specs, dbt lineage graph screenshot, and the data quality baseline vs the As-Is null rates
