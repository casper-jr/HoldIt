# HoldIt — a stock-screening data warehouse

An end-to-end analytics warehouse that turns raw market data into a tested,
version-comparable stock-screening model. Built on **BigQuery + dbt + Airflow**
following a **medallion architecture** (Bronze → Silver → Gold), it ingests five
years of fundamentals and prices for **~950 US stocks**, reconstructs a weekly
point-in-time history, and scores every stock under two competing valuation models —
all defined as code and guarded by data-quality tests.

> This is a deliberate rebuild of an earlier Python/PostgreSQL screener. The rebuild
> exists to fix a class of bug the old system couldn't see and to demonstrate a
> production-shaped data-engineering + analytics stack. The engineering decisions and
> the reasoning behind them live in [`docs/architecture.md`](docs/architecture.md); the
> frozen analysis of the old system's failures is in [`docs/as-is.md`](docs/as-is.md).

---

## What it does

1. **Ingests** raw payloads from yfinance (quote, 5-year price history, financial
   statements, dividends) into an **immutable Bronze layer** — verbatim JSON, nothing
   computed.
2. **Parses and conforms** in **Silver**: prices, a reconstructed weekly fundamentals
   history, an SCD2 company dimension, and dividend history — each at its own grain,
   each tested.
3. **Computes and scores** in **Gold**: seven valuation metrics, two scoring models
   (v1 and v2) expressed as CSV rules, and six analytics marts.
4. **Orchestrates** the whole path weekly with a single Airflow DAG, with a hard
   data-quality gate between Silver and Gold.

---

## Architecture

```
  yfinance ──► BRONZE (holdit_bronze)          raw JSON, immutable, partitioned by date
              raw_yf_quote / price_history /    Python moves data — derives nothing
              financials / dividends            missing is NULL, never 0.0
                        │
                    dbt │ (parse only)
                        ▼
              SILVER (holdit_silver)            conform · type · historize
              stg_yf__*  ·  dim_company (SCD2)   price_date from the payload index
              fct_price_daily                    fundamentals reconstructed onto a
              fct_financials_snapshot            weekly grid with a filing-lag rule
              fct_dividend_history               ⛔ dbt_test_silver — hard gate
                        │
                    dbt │ (transform)
                        ▼
              GOLD (holdit_gold)                metrics · scoring · marts
              fct_metrics → int_metrics_unpivot  scoring is CSV rules, not code
              → fct_metric_scores (v1, v2)       both models coexist and are compared
              fct_valuation_daily (as-of join)
              mart_* (6)                         the only tables Tableau reads
```

Every table is defined by a `.sql` file in this repo (`dbt/`). BigQuery is the
materialization; the repo is the source of truth. Nothing is authored in the console.

---

## The data (current build)

| | |
|---|---|
| Universe | **954 US stocks** (NASDAQ + NYSE, top ~1000 by market cap) |
| History | **5 years** of daily prices · ~4 years of annual fundamentals |
| Payload coverage | **100%** (954/954 on every endpoint, 0 null payloads) |
| Rows | 1.15M daily prices · 1.15M daily valuations · 1,043 weekly metric rows |
| Models | 8 Silver + 10 Gold, built by dbt |
| Tests | **54 dbt tests, all passing** on the production datasets |

---

## What makes it more than a pipeline

**Point-in-time reconstruction with lookahead control.** The old system stored one row
per stock that was overwritten weekly — no history. Here, `fct_financials_snapshot`
rebuilds a weekly grid five years back and attaches, for each synthetic week, only the
annual filing that was *knowable* then (fiscal year-end + a 90-day US filing lag). A
dbt test (`assert_no_lookahead`) makes this mandatory: no row may carry fundamentals
that filed after its own date. Reconstructed rows are labelled and **never earn a
score** — a separate test (`assert_scores_are_live_only`) enforces that an estimate
never becomes a grade.

**Scoring as data, not code.** Two Buffett/Lynch-inspired models (v1, a legacy
value-and-dividend model; v2, which adds ROE, FCF yield, PEG and debt) are encoded as
rows in `seed_scoring_rules.csv` and applied by a single range join. Adding or changing
a model is editing a CSV — no Python classes, no re-deploy. Both versions live in one
table, so `mart_model_comparison` can rank the same universe under both and quantify
where they disagree.

**Missing is NULL, never 0.0.** The single worst bug in the old system was defaulting a
failed fetch or a non-meaningful ratio to zero, which silently poisoned every average
and rank. Here a failed fetch records its status and leaves the value null; every ratio
goes through a `safe_divide` macro. The result is *higher* measured null rates than the
old system reported — because the nulls are now honest rather than hidden.

---

## Data-quality baseline vs the old system

Null rate per metric on the full universe (`mart_data_quality`, 954 stocks):

| Metric | Null rate | Note |
|---|---:|---|
| PEG | 40.0% | null unless PER **and** EPS growth are both positive — the true rate |
| Dividend yield | 23.3% | genuine non-payers |
| PBR / PER / FCF yield | 2–3% | missing net income or equity |
| ROE / debt ratio | ~1% | |
| Dividend-growth years | 0.0% | computed from real dividend history |

The old system reported PEG null at ~19% and dividend-growth-years zero at ~16% — both
**artificially low**, because missing values were defaulted to 0.0 and 39 silent
`except` blocks swallowed the rest. Surfacing the true 40% PEG-null rate is the point:
you cannot trust a screen whose gaps are invisible.

---

## What you can analyze

The six Gold marts are shaped around concrete questions:

- **Model disagreement** (`mart_model_comparison`) — v1 vs v2 rank correlation is
  **Spearman 0.616**; the sharpest splits are utilities like **FirstEnergy / Exelon**
  (v1 loves their dividends, v2 penalizes their debt) versus profitable, low-debt,
  no-dividend names like **Incyte / Exelixis / Zoom** (v2 rewards what v1 ignores).
- **Sector-relative valuation** (`mart_sector_valuation`) — each stock's PER/PBR/ROE vs
  its sector median and vs the sector's own 3-year median, across 11 sectors.
- **PER vs its own history** (`mart_price_history`) — daily valuation percentile,
  drawdown from the 1-year high, and realized volatility per stock.
- **Screening leaderboard** (`mart_leaderboard`) — normalized 0-100 score, grade, and
  rank, per model, per week.
- **Score trend** (`mart_score_history`) and **data quality** (`mart_data_quality`)
  tracked over time.

---

## Scoring models

Both models score a stock 0–100 (normalized so their different maximums are
comparable), then grade A/B/C/D. The rules are Buffett/Lynch-inspired: reward
profitability and cash generation, punish debt and value traps (a low PER with no
growth). v1 is the legacy value-and-dividend model; v2 adds the quality and safety
signals below. All thresholds live in `dbt/seeds/`.

| Metric | v2 max | Idea |
|---|---:|---|
| ROE | 15 | Sustained high ROE signals a moat (Buffett) |
| PER | 10 | Cheap earnings, but capped so low-PER-no-growth can't dominate |
| FCF yield | 10 | Real cash generation, not accounting profit |
| PEG | 10 | Growth-adjusted value (Lynch) |
| Debt ratio | 10 | Balance-sheet safety |
| Dividend yield + growth | 20 | Shareholder return and its consistency |
| Economic moat | 10 | Human qualitative input, via a seed CSV |
| PBR | 5 | ROE-linked: high-ROE firms justify a premium above book |

---

## Stack

| Layer | Technology |
|---|---|
| Warehouse | Google BigQuery (`asia-northeast3`) |
| Transformation | dbt-core + dbt-bigquery (dev/prod targets) |
| Ingestion | Python (yfinance → GCS → BigQuery load) |
| Orchestration | Apache Airflow (LocalExecutor, Docker Compose) |
| Storage | Google Cloud Storage (raw landing) |
| Secrets | Google Secret Manager |
| Dashboard | Tableau (reads `holdit_gold`) |

---

## Repository layout

```
ingestion/          Python — moves raw payloads to Bronze, derives nothing
  main.py           CLI: fetch one (source, endpoint) for a snapshot date
  sources/          yfinance client (DART client stubbed for a future KR phase)
bronze/             Bronze table DDL (dbt reads Bronze but never builds it)
dbt/
  models/silver/    stg_yf__* (parse) · dim_company · fct_* (conform)
  models/gold/      fct_metrics · fct_metric_scores · mart_* (analytics)
  seeds/            scoring rules, model definitions, qualitative moat — as CSV
  snapshots/        snap_company (SCD2 engine)
  tests/            lookahead / live-only / not-future assertions
airflow/            Docker Compose stack + the weekly DAG
docs/
  architecture.md   the full To-Be spec and the decisions behind it
  plan.md           the ordered build worklist
  as-is.md          frozen analysis of the old system's failures
```

---

## Running it

```bash
# dbt (local dev target → *_dev datasets)
cd dbt
GCP_PROJECT=<project> DBT_PROFILES_DIR=. dbt build --target dev

# ingestion (writes raw payloads to GCS, then load to Bronze)
GCP_PROJECT=<project> HOLDIT_RAW_BUCKET=<bucket> \
  python -m ingestion.main --source yf --endpoint quote --snapshot-date 2026-07-15 --limit 1000

# Airflow (the weekly path, dbt runs in-container)
cd airflow && docker compose up
```

Authentication uses Google Application Default Credentials; no credentials live in the
repo.

---

## Status

US path complete: Bronze → Silver → Gold built and tested at full scale, with a Tableau
analysis layer on top. A Korean (DART) source is a possible second phase, gated on a
data-coverage probe — the Silver models were designed to conform a second market
without changing anything downstream.
