# Architecture — the To-Be spec

> The stable reference for the system's design. For the build sequence, see `plan.md`;
> for the analysis of the old system that motivated it, see `as-is.md`.

## Purpose
This is the design specification for the rebuilt system: a cloud data warehouse and
analytics layer for stock screening, built on the medallion pattern with dbt, BigQuery,
and Airflow. It defines every layer, table, grain, and invariant — and records the
decisions behind them, including the alternatives that were considered and rejected.

## Data Architecture
- ### Architecture Diagram

```
  SOURCE                 ┌──────────────┐   ┌──────────────┐
                         │   DART API   │   │   yfinance   │
                         └──────┬───────┘   └──────┬───────┘
                                └─────────┬────────┘
                                          │
  EXTRACT                 ┌───────────────▼────────────────┐
  Python                  │  ingestion package             │  fetch → serialize → write
  Cloud Run Job           │  moves data, decides nothing   │  no derive · no default · no abs()
                          └───────────────┬────────────────┘
                                          │
  LAND                    ┌───────────────▼────────────────┐
  GCS                     │  gs://holdit-raw/              │  {source}/{endpoint}/
                          │  NDJSON                        │  snapshot_date={ds}/{ticker}.json
                          └───────────────┬────────────────┘
                                          │
                                          │  bq load · WRITE_TRUNCATE on the {ds} partition
  ═══════════════════════════════════════ │ ═══ BigQuery ═════════════════════════════════
  the only source of truth                │
                                          │
  BRONZE                  ┌───────────────▼────────────────┐
  landing                 │  holdit_bronze                 │  payload JSON, verbatim
  written by Airflow      │  raw_yf_quote·raw_yf_price_hist│  no UPDATE, no row DELETE
  never by dbt            │  raw_dart_fnltt · raw_*        │  a partition is replaced only
                          └───────────────┬────────────────┘  by re-running its own date
                                          │
                                          │  JSON_VALUE ← the only place JSON is parsed
                                          │
  SILVER / staging        ┌───────────────▼────────────────┐
  parse                   │  stg_yf__quote                 │  one model per bronze table
                          │  stg_yf__price_history         │  extract · cast · rename
                          │  stg_dart__fnltt · stg_*       │  unnest allowed, logic is not
                          └───────────────┬────────────────┘
                                          │
  SILVER / core           ┌───────────────▼────────────────┐
  conform                 │  dim_company (SCD2)            │  KR + US unioned
  three cadences,         │  fct_price_daily               │  daily  · ticker × price_date
  one weekly DAG          │  fct_financials_snapshot       │  weekly · ticker × snapshot_date
                          │  fct_dividend_history          │  yearly · ticker × fiscal_year
                          └───────────────┬────────────────┘
                                          │
                                     ⛔ dbt_test_silver — failure stops the DAG here
                                          │
  GOLD                    ┌───────────────▼────────────────┐      ┌──────────────────┐
  compute + score         │  fct_metrics                   │      │  seeds/          │
                          │  int_metrics_unpivot           │◀─────│  scoring_rules   │
                          │  fct_metric_scores             │      │  scoring_models  │
                          │  mart_leaderboard · mart_*     │      │  qualitative_moat│
                          └───────────────┬────────────────┘      └──────────────────┘
  ═══════════════════════════════════════ │ ═══ end BigQuery ═════════════════════════════
                                          │
  SERVE                   ┌───────────────▼────────────────┐
  Tableau                 │  reads holdit_gold only        │  read-only IAM makes it structural
                          └────────────────────────────────┘


  dbt      builds everything from stg_* downward. Bronze is a source, never a model
  Airflow  drives every arrow, all tasks keyed on {{ ds }}, none call date.today()
           ingest_us → ingest_kr → load_bronze → dbt_run_silver → dbt_test_silver
           → dbt_run_gold → dbt_test_gold → refresh_tableau

  Rule     every box reads ONLY from the box directly above it, and nothing flows backward
           silver reads bronze · gold reads silver · tableau reads gold · never further back
```

- ### Design Principles
	- **Layer isolation.** Silver reads Bronze, Gold reads Silver, Tableau reads Gold. Never further back, never backward. When a source field changes shape the fix lands in exactly one Silver model and everything above it keeps working — the As-Is failure is the opposite, with raw parsing scattered across `fetcher.py`, `processor.py`, and `backtest.py`
	- **Python moves data, SQL transforms data.** Ingestion makes no decisions : no derived values, no defaults, no `abs()`, no dropped fields, no fallbacks. Serializing is not transforming — converting a DataFrame to JSON so it can be stored is mechanical and allowed; computing a CAGR from it is a decision and belongs in SQL
	- **Missing is not zero.** A failed fetch or parse produces `NULL` plus a recorded `http_status`, never `0.0`. This one rule kills the As-Is bug where `capital_expenditure = 0.0` inflates FCF yield into 10 undeserved points
	- **Raw is immutable, because loss is irreversible.** A wrong parse in Silver is one `dbt run --full-refresh` away from fixed. A wrong parse before Bronze is permanent, because no API will return last Friday's snapshot. This is the entire argument for parsing in Silver rather than at ingest, and it is why schema drift becomes a free upgrade instead of silent data loss
	- **Everything is re-runnable by date.** Every task is keyed on `snapshot_date` and every load overwrites its own partition, so any date can be re-run or backfilled without duplicating
	- **Repo = definition, GCP = materialization.** Every query's source of truth is a `.sql` file in git. Nothing is authored in the BigQuery console — no scheduled queries, no saved views, no stored procedures. BigQuery holds only what those files produce
	- **The asymmetry that justifies all of the above.** Delete `holdit_silver` and `holdit_gold` and nothing is lost — `dbt build` rebuilds them from the repo plus Bronze. Delete Bronze and the repo cannot save me. Everything downstream of Bronze is disposable *because* it is derivable from code; Bronze is the only irreplaceable thing in the system
- ### Component Stack
	- Ingestion : Python 3.11, `yfinance` + DART Open API, containerized, run as a Cloud Run Job. Fetch and write, nothing else
	- Raw landing : GCS `gs://holdit-raw/`, NDJSON, pathed `{source}/{endpoint}/snapshot_date={ds}/{ticker}.json`
	- Warehouse : BigQuery, datasets `holdit_bronze` / `holdit_silver` / `holdit_gold`. The only source of truth
	- Transformation : dbt-core + `dbt-bigquery`, running inside the Airflow container. Builds every Silver and Gold model
	- Orchestration : Apache Airflow, self-hosted in Docker Compose locally
	- BI : Tableau Desktop, connected to `holdit_gold`
	- Secrets : GCP Secret Manager (DART API key, service account credentials). No `.env`, no credentials in the repo
	- Version control : GitHub — ingestion package, dbt project, Airflow DAGs, seed CSVs
	- Deferred : Terraform for the GCP resources. Worth adding as an IaC talking point, but not before the pipeline works end to end
	- dbt runs locally but computes nothing — it compiles Jinja+SQL and sends plain SQL to BigQuery, which does the work. dbt is a client, not an engine, which is why `dbt run` on a laptop can build a warehouse. A model is therefore *not* pasteable into the console; the compiled version lands in `target/compiled/`
	- `ref()` is why models must be files in a project rather than queries in a console : `{{ ref('fct_financials_snapshot') }}` is what tells dbt that Gold depends on Silver, and that graph is what produces the build order, the lineage graph, and the correct cascade on `--full-refresh`. Hardcode the table name and dbt goes blind
	- The console still has a role : ad-hoc exploration and debugging. The rule is that **if a query needs to run more than once, it graduates into a model in the repo.** `bq_analytics.py` is that graduation done wrong — repeated analytical queries trapped in Python string constants where nothing can test them or know what they depend on
- ### Repository Structure

```
holdit/
│
├── ingestion/                              ← the ONLY code that touches an API
│   ├── __init__.py
│   ├── sources/
│   │   ├── yfinance_client.py              fetch, return payload as-is
│   │   └── dart_client.py                  fetch, return response.text + http_status
│   ├── serialize.py                        dict/DataFrame → NDJSON (to_json orient='split')
│   ├── gcs_writer.py                       clear prefix, then write snapshot_date={ds}
│   ├── universe.py                         which tickers a run fetches (US screener; KR in Step 6)
│   ├── config.py                           Secret Manager, no .env
│   └── main.py                             CLI: --source --endpoint --snapshot-date
├── Dockerfile                              Cloud Run Job image
├── requirements.txt
│
├── bronze/
│   └── create_bronze_tables.sql            Bronze DDL — dbt never builds Bronze, so it lives here
│
├── dbt/
│   ├── dbt_project.yml                     maps folders → datasets + materializations
│   ├── profiles.yml                        env_var() only, never literal credentials
│   ├── packages.yml                        dbt_utils
│   │
│   ├── models/
│   │   ├── bronze/                         ══ LAYER 1 ══
│   │   │   └── _bronze__sources.yml        no .sql files, ever. bronze is a source,
│   │   │                                   not a model. dbt reads and tests it,
│   │   │                                   dbt never builds it
│   │   ├── silver/                         ══ LAYER 2 ══
│   │   │   ├── staging/                    parse only, one model per bronze table
│   │   │   │   ├── _silver_staging__models.yml
│   │   │   │   ├── stg_yf__quote.sql
│   │   │   │   ├── stg_yf__price_history.sql   unnests the OHLCV range
│   │   │   │   ├── stg_yf__financials.sql
│   │   │   │   ├── stg_yf__dividends.sql
│   │   │   │   ├── stg_dart__fnltt.sql
│   │   │   │   ├── stg_dart__alot_matter.sql
│   │   │   │   ├── stg_dart__stock_totqy.sql
│   │   │   │   └── stg_dart__disclosure.sql
│   │   │   └── core/                       conform, type, historize
│   │   │       ├── _silver_core__models.yml
│   │   │       ├── dim_company.sql         reads snapshots/snap_company
│   │   │       ├── fct_price_daily.sql     the ONLY price source
│   │   │       ├── fct_financials_snapshot.sql
│   │   │       └── fct_dividend_history.sql
│   │   └── gold/                           ══ LAYER 3 ══
│   │       ├── _gold__models.yml
│   │       ├── metrics/
│   │       │   ├── fct_metrics.sql         weekly · replaces processor.py
│   │       │   ├── fct_valuation_daily.sql daily · price ratios, not scored
│   │       │   └── int_metrics_unpivot.sql (ticker, snapshot_date, metric, value)
│   │       ├── scoring/
│   │       │   └── fct_metric_scores.sql   replaces scorers/ — range join to seeds
│   │       └── marts/                      the only models Tableau may read
│   │           ├── mart_leaderboard.sql
│   │           ├── mart_model_comparison.sql
│   │           ├── mart_score_history.sql
│   │           ├── mart_price_history.sql
│   │           ├── mart_sector_valuation.sql
│   │           └── mart_data_quality.sql
│   │
│   ├── seeds/                              human input + opinions, git-tracked
│   │   ├── _seeds__properties.yml
│   │   ├── seed_scoring_rules.csv          model_version, metric, min, max, points
│   │   ├── seed_scoring_models.csv         model_version, metric, max_points
│   │   ├── seed_qualitative_moat.csv       replaces the Excel =W2+X2 loop
│   │   └── seed_exchange_mapping.csv       NYQ→NYSE, NMS→NASDAQ, KOSPI, KOSDAQ
│   ├── snapshots/
│   │   └── snap_company.sql                SCD2 engine behind dim_company
│   ├── macros/
│   │   ├── safe_divide.sql                 every ratio in fct_metrics uses it
│   │   └── generate_schema_name.sql        dev/prod dataset routing
│   └── tests/
│       ├── assert_per_within_range.sql
│       ├── assert_debt_ratio_non_negative.sql
│       ├── assert_snapshot_date_not_future.sql
│       ├── assert_no_lookahead.sql             asof_date <= snapshot_date
│       ├── assert_scores_are_live_only.sql     no grade from a reconstruction
│       └── assert_reconstruction_is_labelled.sql
│
├── airflow/
│   ├── docker-compose.yml
│   ├── Dockerfile                          base image + ingestion deps (runs ingestion in-container for now)
│   ├── requirements-airflow.txt
│   └── dags/
│       └── holdit_weekly.py                the only orchestrator
│
├── docs/
│   ├── architecture.md                     this file
│   ├── plan.md                             the worklist
│   └── as-is.md                            frozen problem analysis
├── CLAUDE.md
└── README.md
```

- `_bronze__sources.yml` having no `.sql` files beside it is the layer rule made physical — there is nowhere in the repo a Gold model *could* be written against Bronze
- `ingestion/` has no BigQuery dependency at all, matching the IAM design where `sa-holdit-ingest` cannot write to BigQuery even if the code tried
- The tree is only folders until `dbt_project.yml` binds them to datasets and materializations. Note it has no `bronze:` entry, because dbt has nothing to build there

```yaml
models:
  holdit:
    silver:
      staging:
        +schema: silver
        +materialized: incremental
        +partition_by: {field: snapshot_date, data_type: date}
        +cluster_by: [ticker]
      core:
        +schema: silver
        +materialized: incremental
        +partition_by: {field: snapshot_date, data_type: date}
        +cluster_by: [ticker]
    gold:
      +schema: gold
      metrics:
        +materialized: incremental
        +partition_by: {field: snapshot_date, data_type: date}
        +cluster_by: [ticker]
      scoring:
        +materialized: incremental
        +partition_by: {field: snapshot_date, data_type: date}
        +cluster_by: [ticker]
      marts:
        +materialized: table        # Tableau reads these; volume is trivial
```

- Staging materializes to a `table` rather than a view specifically because JSON parsing is the expensive operation — as views, every Gold query would re-parse all history and BigQuery would bill for the scan each time. **Step 4 built Silver as `table` (full rebuild), not `incremental`** : with a single seeded snapshot, `incremental` insert-overwrite buys nothing and the reconstruction model is a full rebuild by nature. Moving to `incremental` once the weekly DAG accumulates real partition volume is a config change, not a SQL rewrite
- **The daily models override the folder-level partition key.** `fct_price_daily` and `fct_valuation_daily` are grained on `price_date`, not `snapshot_date`, so each carries its own `{{ config(partition_by={'field': 'price_date', 'data_type': 'date'}) }}`. Inheriting the folder default would partition them on a column they do not have. `dim_company` and `fct_dividend_history` have no date grain at all and set `{{ config(partition_by=none) }}`
- `dim_company` is the other exception : it is a dbt `snapshot`, because SCD2 is what snapshots exist for
- `dbt run --full-refresh --select stg_yf__quote+` is the escape hatch and the whole payoff of keeping Bronze raw

- ### Storage Layout

```
gs://holdit-raw/
├── yf/
│   ├── quote/
│   │   ├── snapshot_date=2026-07-17/
│   │   │   ├── AAPL.json
│   │   │   ├── MSFT.json
│   │   │   └── ...
│   │   └── snapshot_date=2026-07-24/
│   ├── price_history/                  payload = a RANGE of daily OHLCV rows
│   ├── financials/
│   └── dividends/
└── dart/
    ├── fnltt/
    │   └── snapshot_date=2026-07-17/
    │       └── 005930.json
    ├── alot_matter/
    ├── stock_totqy/
    └── disclosure/

BigQuery
├── holdit_bronze     raw_yf_quote, raw_yf_price_history, raw_yf_financials,
│                     raw_yf_dividends, raw_dart_* (4)          — 8 tables
├── holdit_silver     stg_* (7), dim_company, fct_price_daily,
│                     fct_financials_snapshot, fct_dividend_history
├── holdit_gold       fct_metrics, fct_valuation_daily, int_metrics_unpivot,
│                     fct_metric_scores, mart_* (6)
├── holdit_silver_dev \ dev targets — same bronze, separate outputs
└── holdit_gold_dev   /
```

- `snapshot_date={ds}` is Hive-style partitioning and is load-bearing, not cosmetic : it lets the load job target one date's partition with `WRITE_TRUNCATE`, which makes re-runs idempotent, which is what lets `fetch_history` be deleted entirely
- Ingestion **clears the `snapshot_date={ds}` prefix before writing it.** Object-level overwrite alone is not enough : a re-run with a shorter ticker list would leave orphan objects from the previous attempt, and the load job would pick them up

- ### Conventions
	- Bronze table : `raw_{source}_{endpoint}` — `raw_yf_quote`, `raw_dart_fnltt`
	- Staging model : `stg_{source}__{entity}` — double underscore separates source from entity. **1:1 means one staging model per Bronze table, not one output row per input row.** `stg_yf__price_history` unnests a range payload into one row per trading day, and that is still parsing — unnesting a JSON array is extraction, not a decision
	- Silver core : `dim_{entity}`, `fct_{grain}` · Gold : `int_{purpose}`, `fct_{grain}`, `mart_{subject}` · Seeds : `seed_{subject}`
	- `stg_` means "parses a source". A model that does not read Bronze is never named `stg_`
	- Sources are `yf` and `dart` everywhere — in GCS paths, Bronze tables, and model names. Never `yfinance` in an identifier
	- Every model declares its grain in `schema.yml`, and every declared grain has a `unique` test on it. **A grain that is not tested is not a grain**
- ### Environments, Access, Cost
	- Two dbt targets against one GCP project : `dev` → `holdit_silver_dev` / `holdit_gold_dev`, `prod` → `holdit_silver` / `holdit_gold`. Airflow runs `prod`; local development runs `dev`, so a broken model in progress can never reach the dashboard
	- Both targets read the same Bronze. Bronze is expensive to reproduce (real API calls against rate limits) and impossible to regenerate for a past date, so it is never duplicated per environment
	- `sa-holdit-ingest` (Cloud Run Job) : Storage Object Admin on the raw bucket, Secret Accessor for the DART key. **No BigQuery access at all** — ingestion has no reason to touch the warehouse
	- `sa-holdit-orchestrate` (Airflow) : Cloud Run Invoker, BigQuery Job User, BigQuery Data Editor on the datasets, Storage Object Viewer on the raw bucket
	- Tableau : read-only account scoped to `holdit_gold`, which enforces the layer rule at the IAM level rather than by convention
	- Cost : Bronze grows a few GB/year (~1200 tickers × 8 endpoints × 52 weeks of JSON), plus a one-off few GB from the 5-year backfill; every other layer is a rounding error. BigQuery's free tier (1 TiB query/month, 10 GiB storage) covers this or it spills into a couple of dollars. Deleting Cloud SQL removes the one always-on billed resource the As-Is project has. The only real cost decision in the architecture is the orchestrator
- ### Worked Example — tracing PER end to end
	- Ten hops, every one inspectable and re-runnable. The As-Is equivalent is three Python files and no way to see any intermediate state
	- 1. yfinance : `Ticker("AAPL").history(start='2026-07-11', end='2026-07-18', auto_adjust=False)` returns a DataFrame of that week's daily OHLCV
	- 2. Extract : `df.to_json(orient='split')` with `snapshot_date=2026-07-17`, `ingested_at=<now>`, `source='yf'`, `endpoint='price_history'`
	- 3. Land : `gs://holdit-raw/yf/price_history/snapshot_date=2026-07-17/AAPL.json`
	- 4. Load : appears in `holdit_bronze.raw_yf_price_history` as **one row** whose `payload` holds the whole week, untouched
	- 5. Parse : `stg_yf__price_history` unnests that payload into **seven rows**, one per trading day, with `close` and `adj_close` kept separate
	- 6. Conform : `fct_price_daily` gets the row (`AAPL`, `2026-07-17`, close). Separately, `fct_financials_snapshot` emits one row for (`AAPL`, `2026-07-17`) with `net_income`, `shares_outstanding` and their own period-end dates — no price
	- 7. Compute : `fct_metrics` joins the two on `snapshot_date`, calculates `eps = net_income / shares_outstanding`, then `per = close / eps`
	- 8. Unpivot : `int_metrics_unpivot` emits (`AAPL`, `2026-07-17`, `'per'`, `28.4`)
	- 9. Score : `fct_metric_scores` range-joins to `seed_scoring_rules` and awards 0 points, because the v2 rule row says PER ≥ 15 scores 0
	- 10. Serve : `mart_leaderboard` rolls the metric scores into a total, a normalized 0-100 score, a grade, and a rank, and Tableau reads it
- ### Failure and Recovery
	- **Per-ticker API failure** : recorded in Bronze with its `http_status` and a null payload, and the run continues. One bad ticker must never kill a 1200-ticker run. `mart_data_quality` surfaces the coverage drop
	- **Systemic failure** (auth, quota, network) : raised, so the Airflow task fails, retries with backoff, and stops the DAG rather than landing a silently empty snapshot
	- **`dbt_test_silver` fails** : the DAG stops before Gold. The marts keep last week's data, so the dashboard is stale-but-correct rather than fresh-but-wrong
	- **Load interrupted midway** : re-run the task. Partition overwrite makes the retry safe by construction — which is exactly what the As-Is calendar-day `fetch_history` dedupe actively prevents
	- **Wrong parse discovered later** : fix the staging model, `dbt run --full-refresh --select stg_yf__quote+`, and all history rebuilds from Bronze. No re-fetch, no data loss
	- **Machine off on Friday** : `airflow dags backfill` for the missed dates. Prices are fully recovered; the fundamentals caveat below applies
	- **Prices backfill perfectly. Fundamentals do not.** `history(start, end)` is a true point-in-time series, so price depth is exact. Reconstructed fundamentals are estimates, and the project builds them anyway — labelled
	- **The rule that keeps this honest** : reconstructed history feeds *analysis and charts*. It never feeds `fct_metrics`, never feeds `fct_metric_scores`, and never produces a grade. Scoring runs on live snapshots only, from launch forward. A score is a claim; an estimate is not allowed to make one. This is the line `backtest.py` never drew, and `assert_scores_are_live_only` enforces it
	- **Lookahead bias is controlled**, because it is cheap to control and it would corrupt the charts directly — FY2022 figures leaking into a January 2023 price date would distort "PER vs its own history" for every ticker. The filing-lag rule handles it : real `rcept_dt` for KR, a 90-day assumption for US. `assert_no_lookahead` makes it mandatory rather than advisory
	- **Known and accepted, not worked around** : reconstructed fundamentals are as-*restated* rather than as-first-reported, and the backfill covers only currently-listed tickers, so pre-launch aggregates describe the past of today's survivors. Both would matter if this project claimed a strategy return — it does not, since no reconstruction can produce a score. For descriptive valuation context on live tickers the distortion is small, and correcting either one needs a paid point-in-time dataset. Documented, deliberately not solved
	- **How reconstruction is marked** : `is_reconstructed` on the fundamentals row, `is_backfilled` on the price row, `fundamentals_asof_date` for which filing was used, and `ingested_at` far exceeding `snapshot_date` as the underlying tell
- ### Architecture Decisions
	- **Rejected — keeping Cloud SQL as the serving DB.** System of record plus a lagging BigQuery copy is two sources of truth, the exact problem this refactor exists to remove. BigQuery serves Tableau directly at this volume
	- **Rejected — Cloud Composer.** ~$300/month for a weekly DAG over 1200 rows. Self-hosted Airflow costs nothing and demonstrates the same skill. The tradeoff is that a missed week needs a backfill
	- **Rejected — parsing JSON before Bronze.** Irreversible on a wrong parse; turns schema drift into silent permanent data loss. Parsing in Silver staging costs one hop and buys full re-derivability
	- **Rejected — one SQL model per scoring version.** Better than Python classes, but still code per model. Thresholds as seed rows make a new version a CSV edit, and make `mart_model_comparison` work for models that do not exist yet
	- **Rejected — Spark, Dataflow, any distributed engine.** ~1200 rows/week. Reaching for Spark here is a red flag about judgment, not a demonstration of skill
	- **Rejected — streaming / Kafka.** Fundamentals update quarterly and the screening question is long-term. Weekly batch is not a compromise, it is the right cadence for the domain
	- **Rejected — Snowflake / Databricks.** GCP is a stated requirement drawn from the target job postings
	- **Rejected — dbt Cloud.** Its free tier would work, but dbt-core inside Airflow keeps orchestration in one place and avoids two schedulers — the same reason Cloud Scheduler is deleted
	- **Chosen — BigQuery native `JSON` type for payloads, over `STRING`.** Validated at load, stored efficiently, queryable with `JSON_VALUE` without a `PARSE_JSON` wrapper. Tradeoff : malformed JSON fails the load rather than landing; acceptable because serialization is controlled here and DART returns well-formed JSON
	- **Chosen — Airflow, justified by retries + the DQ gate + dependency management**, with backfill for missed weeks as a secondary benefit. A weekly cron alone would not justify it, and backfill alone is a weaker argument than it first appears given the fundamentals limit above
	- **Chosen — daily prices, weekly fundamentals, weekly DAG.** Three cadences, one schedule. `yf.Ticker().history(start, end)` returns a whole date range in a single call, so the weekly run fetches the week's seven days of prices in the same 1200 calls it already makes. A daily price series therefore costs **no extra schedule** — which matters, because a daily DAG would require an always-on scheduler and break the local-Airflow decision above
	- **Chosen — deep backfill on day one : 5 years of prices, ~4 years of reconstructed fundamentals.** `history()` backfills prices cleanly, so price depth is free and exact. Valuation depth is not free — prices are only half of PER, and without reconstructed fundamentals every trailing-median question would be unanswerable until 2029. The reconstruction is therefore built deliberately, with survivorship / restatement / lookahead bias accepted and labelled (see Failure and Recovery). Fundamentals reach ~4 years because that is what yfinance annuals return, so valuation history is bounded by the shorter of the two
	- **The boundary that makes it defensible** : reconstructed history is allowed to inform analysis and charts, never to produce a score. `fct_metrics` and `fct_metric_scores` read live snapshots only. This is the line `backtest.py` never drew
	- **Rejected — daily scoring.** Fundamentals move quarterly; scoring daily would produce 250 near-identical rows a year per ticker and imply a precision the inputs do not have. Cadence follows the decision being made, not the fastest-moving feed

## Requirements
- ### Bronze
	- One table per source endpoint : `raw_yf_quote`, `raw_yf_price_history`, `raw_yf_financials`, `raw_yf_dividends`, `raw_dart_fnltt`, `raw_dart_alot_matter`, `raw_dart_stock_totqy`, `raw_dart_disclosure`
	- Common columns : `snapshot_date DATE`, `ingested_at TIMESTAMP`, `source STRING`, `endpoint STRING`, `ticker STRING`, `request_params JSON`, `http_status INT64`, `payload JSON`
	- `payload` is the untouched response body. `snapshot_date` is the business date the row describes; `ingested_at` is when it was actually fetched. They are separate columns and never conflated
	- Partitioned by `snapshot_date`, clustered by `ticker`. **No row is ever UPDATEd and no row is ever DELETEd individually** — a partition is replaced wholesale by `WRITE_TRUNCATE`, and only ever by re-running its own date. That is the precise sense in which Bronze is append-only
	- `raw_yf_price_history` is the one table whose payload holds a **range** rather than a point : one Bronze row per (`ticker`, `snapshot_date`) containing a span of daily OHLCV. `request_params` records the `start`/`end` actually requested, so the range a row covers is self-describing
		- For this table `snapshot_date` is **the as-of date of the request**, not the date of the data inside. A weekly run's row covers 7 days; the one-time backfill row covers 5 years
		- **`price_date` is derived from the payload index, never from `snapshot_date`.** Getting this backwards would collapse 5 years of prices onto a single day
	- Fetch prices with `auto_adjust=False` and keep **both** `Close` and `Adj Close`. Adjusted close is retroactively rewritten by every split and dividend, so a PER computed from it is wrong. Raw close values valuation; adjusted close values returns. Bronze keeps both and Silver decides
	- `raw_yf_financials` serves two eras : the weekly live fetch, and a one-time backfill of the annual `income_stmt` / `balance_sheet` / `cashflow` history. yfinance returns roughly **4 years of annuals**, which is what bounds reconstructed valuation history — prices reach 5 years, fundamentals do not
	- **Timing asymmetry between markets.** The `0 8 * * 5` run is 17:00 KST / 08:00 UTC. KRX closed at 06:30 UTC, so Friday's KR close is available. US markets open at 13:30 UTC, so Friday's US close does **not** exist yet and arrives in the following week's run. The US price series therefore lags one trading day by construction. This is expected, not a bug, and `fct_metrics` absorbs it via the last-trading-day fallback
	- yfinance boundary : yfinance's own output is "raw", not Yahoo's HTTP response. `json.dumps(ticker.info)` is the payload. DataFrames use `to_json(orient='split')`, which preserves the index — `orient='records'` drops it, and those index labels *are* the dates and fiscal period ends
	- DART boundary : the response body verbatim, plus `http_status`, including on failures. A DART `status != '000'` is data about pipeline health, not something to swallow
- ### Silver
	- `dim_company` — SCD2 via dbt snapshot. `ticker`, `name`, `market`, `sector`, `industry`, `currency`, `financial_currency`, `is_delisted`, `valid_from`, `valid_to`, `is_current`
		- **SCD2 history accrues forward from launch, not backward from Bronze.** dbt `snapshot` compares each run to the stored table and opens a new era when a checked column changes; it does not read historical `snapshot_date` partitions and synthesize eras from them. This is deliberate — company attributes rarely change and launch-forward history matches the live-only stance — so on first build every ticker has one current row. `is_delisted` is `false` for the whole universe by construction (it is a live screener); real delisting detection arrives with DART in Step 6
		- `market` resolves to real values (`KOSPI`, `KOSDAQ`, `NYSE`, `NASDAQ`) via `seed_exchange_mapping.csv` — never `"KOSPI/KOSDAQ"`, never raw codes (`NYQ`, `NMS`)
		- `sector` / `industry` are added now : sector-relative valuation is the most obvious analysis and is impossible without them
		- `currency` (price) and `financial_currency` (statements) are separate columns — their divergence *is* the ADR case, and it is resolved in SQL here rather than by an API call mid-transform
	- **Three cadences, three grains.** The As-Is crammed all of them into one row and that is problem #2. Each now gets its own table at its own grain, and `fct_metrics` joins them
	- `fct_price_daily` — grain (`ticker`, `price_date`). `close`, `adj_close`, `open`, `high`, `low`, `volume`, `currency`, `is_backfilled`
		- **The only price source in the warehouse.** `raw_yf_quote.currentPrice` is deliberately ignored — quote is kept for its metadata (currency, financial_currency, shares outstanding, sector, industry), not its price. Two price sources would eventually disagree
		- Roughly 1200 tickers × 250 trading days = ~300k rows/year. Trivial for BigQuery
	- `fct_financials_snapshot` — grain (`ticker`, `snapshot_date`). Fundamentals only, **no price column**. This is the change that gives the project a time dimension
		- The remaining mixed timeframes become explicit columns : `income_period_end`, `balance_period_end`. The row no longer pretends they are the same date. Price is no longer among them — it is a join to `fct_price_daily` on `snapshot_date`; dividend timing is not among them either — it lives at its own grain in `fct_dividend_history`, so a `dividend_fiscal_year` column here would only duplicate it (built Step 4, deviation from the original three-column list)
		- **Two eras, one grain.** Post-launch rows are live weekly fetches. Pre-launch rows are *reconstructed* onto the same weekly grid from annual filings, so the table's meaning — "what was knowable about this ticker on this date" — holds for both. The only difference is provenance, carried by `is_reconstructed`
		- Reconstruction rule : for each synthetic weekly `snapshot_date`, attach the most recent annual report whose **filing date** is on or before it. `fundamentals_asof_date` records which filing was used, so the lag is visible in the data rather than assumed
		- **Filing lag must be applied or the whole thing is lookahead bias.** FY2022 figures were not public in January 2023. yfinance exposes no filing date, so US reconstruction assumes annual results become available **90 days after fiscal year end** — an assumption, recorded here because it materially shapes every reconstructed number. DART publishes real filing dates, so KR uses the actual `rcept_dt` and needs no assumption
		- Columns added for this : `is_reconstructed BOOL`, `fundamentals_asof_date DATE`, `fundamentals_granularity STRING`. Granularity is `annual` in both eras as built in Step 4 — the yfinance fetch is annual `income_stmt`/`balance_sheet`/`cashflow`, so live rows are annual too; `ttm`/`quarterly` live granularity is a later enhancement, not a Step 4 deliverable
		- **Reconstruction boundary is a `launch_date` var** (`dbt_project.yml`), set to the first live snapshot (`2026-07-14`). Rows dated before it are the synthetic weekly-Friday grid (`is_reconstructed = true`); rows on/after it are real fetches. The US filing lag is the `us_filing_lag_days` var (90); KR overrides it with real `rcept_dt` in Step 6
	- `fct_dividend_history` — grain (`ticker`, `fiscal_year`). Replaces the "consecutive increase years" integer that was computed at fetch time
	- Staging models parse only : `JSON_VALUE` extraction, unnesting, casting, renaming, and the previously-hidden fallbacks made explicit — `currentPrice` else `previousClose` (`fetcher.py:357`) becomes a visible, testable `coalesce`
	- All columns nullable, no defaults
- ### Gold
	- `fct_metrics` — grain (`ticker`, `snapshot_date`). **Stays weekly**, because scoring is a weekly screening decision. Joins `fct_financials_snapshot` to `fct_price_daily` on `snapshot_date` (falling back to the last trading day when the snapshot lands on a holiday). PER, PBR, ROE, FCF yield, PEG, debt ratio, dividend yield in SQL. **Replaces `processor.py` entirely**
		- **ADR currency resolution.** PER/PBR/FCF-yield divide a price (price currency) by statement figures (financial currency); for a US-listed ADR of a foreign company the two differ and the raw ratio is meaningless (a KRW/IDR earnings number collapses PER toward 0). PER falls back to Yahoo's currency-consistent `trailingPE` from `raw_yf_quote` (in Bronze, so no live API call); PBR and FCF-yield have no reliable currency-consistent source and are left NULL rather than wrong. ROE and debt ratio are currency-safe (both sides in the financial currency). `fct_valuation_daily` has no per-day `trailingPE`, so it NULLs the three affected ratios on a mismatch
	- `fct_valuation_daily` — grain (`ticker`, `price_date`). Price-driven ratios only (PER, PBR, dividend yield, FCF yield), recomputed daily via an **as-of join** : each `price_date` takes the latest `fct_financials_snapshot` row with `snapshot_date <= price_date`. This one join spans both eras — live fundamentals after launch, reconstructed before — and `is_reconstructed` carries through so every downstream row stays labelled. Not scored; scoring reads `fct_metrics`
	- Splitting these two is what keeps a daily price series from forcing a daily scoring run. Fundamentals move quarterly; scoring cadence follows the decision, not the data feed
	- `int_metrics_unpivot` — `fct_metrics` unpivoted to (`ticker`, `snapshot_date`, `metric`, `value`), so one join scores every metric at once
	- `seed_scoring_rules.csv` — thresholds as **data, not code**. `model_version`, `metric`, `min_value`, `max_value`, `points`
	- `seed_scoring_models.csv` — `model_version`, `metric`, `max_points`, `is_qualitative`. Enables normalizing to 0-100 so models with different maximums (v1 = 47pt, v2 = 90pt) become comparable
	- `seed_qualitative_moat.csv` — the human moat score (0-10), git-tracked, joined in Gold. Replaces the Excel `=W2+X2` in a GCS CSV and closes the loop that currently loses the input every week
	- `fct_metric_scores` — range join `int_metrics_unpivot` to the rules on `value >= min_value AND value < max_value`. Grain (`ticker`, `snapshot_date`, `model_version`, `metric`). **Adding a scoring model is adding CSV rows**, not writing a Python class and re-running `score --all`
	- `mart_leaderboard` — grain (`ticker`, `snapshot_date`, `model_version`) : total, normalized score, grade, rank. All model versions coexist in one table, distinguished by `model_version` — not one view per version
	- `mart_model_comparison` — v1 vs v2 vs vN per ticker, rank delta, Spearman correlation. **This is the "compare models by SQL" requirement, delivered**
	- `mart_score_history` — score trend across snapshots. `bq_analytics.py` rising/falling/consistent become models here
	- `mart_sector_valuation` — PER/PBR/ROE vs sector median and vs the sector's own 3-year median. Both halves of the ratio backfill : prices exactly, fundamentals by reconstruction. `is_reconstructed` carries through, and Tableau captions any chart whose window reaches before launch — a reconstructed median is an estimate, and an uncaptioned estimate is a lie
	- `mart_price_history` — daily price and valuation series per ticker with trailing percentiles, drawdown, and volatility. Reads `fct_valuation_daily`
	- `mart_data_quality` — null rate and row count per metric per snapshot, tracked over time as a first-class table
	- **Built Step 5 — deviations from the pure design, deliberate**:
		- The "scoring is data, not code" claim holds for every metric except **v2's PBR score, which depends on ROE** (a high-ROE stock earns a book-value premium). That two-variable rule can't be one `(min,max,points)` row, so it is a `CASE` in `fct_metric_scores` that replaces the v2 pbr range-join row. The other seven metrics stay pure seed rows
		- **`cancel` (share-cancellation) is DART-only** — yfinance does not expose it — so it scores 0 for the US universe now and returns with KR in Step 6. It stays in `seed_scoring_models` (theoretical max) so normalization is honest about the unreachable points
		- **`moat` is added directly from `seed_qualitative_moat`** (0-10), not range-joined — `is_qualitative` in `seed_scoring_models` flags it. This is the fix for the structurally-zero moat bug
		- **Grades are computed on the normalized 0-100 score**, fixing the A>80-on-a-scale-that-never-reaches-100 bug
		- **"Reproduce v2's scores" is validated by construction, not a row diff** — `scorers/` was deleted in Step 1 and `stock_scores` in Step 2, so there is no stored output to diff. The seed faithfully encodes the v2 threshold function and scores were hand-checked (AAPL v2 = 42)
		- `dividend_yield` is left to weekly `fct_metrics`, not `fct_valuation_daily` (a daily per-year dividend as-of join for marginal value). `div_growth_years` is approximate — calendar-year dividend aggregation, so a payment-timing shift can break a streak; it does not affect grades since ≥10 years already caps the points
- ### Orchestration
	- DAG `holdit_weekly`, schedule `0 8 * * 5`, `catchup=True`, retries with exponential backoff on every task
	- `ingest_us` → `ingest_kr` → `load_bronze` → `dbt_run_silver` → `dbt_test_silver` → `dbt_run_gold` → `dbt_test_gold` → `refresh_tableau`
	- Airflow triggers ingestion via `CloudRunExecuteJobOperator`; dbt runs in the Airflow container against BigQuery
	- `ingest_us` and `ingest_kr` are separate tasks so a DART outage cannot block the US path, and so their success rates are tracked independently
	- dbt is split by layer rather than one `dbt build`, purely so `dbt_test_silver` can sit between them as a hard gate
	- Deleted : Cloud Scheduler, `entrypoint.sh`, `fetch_history` and its calendar-day dedupe. Airflow is the only orchestrator
- ### Data Quality
	- dbt tests on every Silver and Gold model : `unique` on the grain, `not_null` on keys, `accepted_values` on `market` and `grade`, `relationships` from facts to `dim_company`
	- Custom tests : PER within a sane band, debt ratio non-negative, `snapshot_date` not in the future, `price_date` not in the future
	- **Tests that enforce the reconstruction boundary** — these are the ones that keep the reconstruction honest, and they are not optional :
		- `assert_no_lookahead` : no `fct_financials_snapshot` row has `fundamentals_asof_date > snapshot_date`
		- `assert_scores_are_live_only` : no `fct_metric_scores` row traces to a fundamentals row with `is_reconstructed = true`. A reconstruction must never earn a grade
		- `assert_reconstruction_is_labelled` : every row with `snapshot_date` before launch has `is_reconstructed = true`
	- Baseline to beat, from the last 3 As-Is exports : PEG null ~19%, dividend-growth-years zero ~16%, 39 silent `except` blocks hiding the rest
- ### Analysis Questions
	- The Gold layer and the dashboard are designed to answer these. Without them, "build a Gold layer" has no acceptance criteria
	- Which sectors are cheapest on PER/PBR relative to their own 3-year median?
	- Do v1 and v2 disagree on ranking, and where? Which metric drives the disagreement most?
	- Which stocks held grade A/B for N consecutive weeks, and what did they have in common?
	- Which stocks moved most in score over 4 weeks, and was it the price or the fundamentals that moved? (Weekly snapshots alone can answer this by checking whether the fundamentals changed between snapshots; daily prices show *when within the week* it happened)
	- Where does a stock's PER sit against its own trailing 3-year distribution — cheap in absolute terms, or merely cheap for itself? (Needs `fct_valuation_daily`; the pre-launch portion is reconstructed. Comparing a ticker to its own history is the case where reconstruction is weakest-biased, since survivorship is irrelevant to a company that still exists)
	- Which A/B-graded stocks are in the largest drawdown from their 1-year high? (Needs `fct_price_daily`. Exact — prices are observed, not reconstructed)
	- What is the coverage and null rate per source per week, and is it getting better or worse?
- ### Tableau Dashboard
	- Reads `holdit_gold` only — the `mart_*` models and the `report_*` views are its source
	- Tableau Public is free but publishes data openly and cannot connect live to BigQuery, so an extract published from Tableau Desktop is the fallback if Desktop's trial expires
