# As-Is — the system being replaced

> Frozen analysis, written before the refactor began. This file is the evidence base for every
> decision in `architecture.md`. It is not maintained — file paths and line numbers below refer
> to the pre-refactor code, recoverable from git history on `main`.

## Purpose
- A Stock-Screening program for US and Korea's stock market

## Problem
- I need to do a Data Analytics project, but this project does not really match the requirements for a job post from companies.
	- Need to refactor the project to a End-to-End project that contains a Data Warehouse(Cloud-base) to Data Analytics project by SQL
- Not really comfortable to compare different models by doing the whole scoring process again. Instead, it will be easier to create different views that can show the results of scoring models by SQL after creating the Data Warehouse

### 1. There is no warehouse — data is transformed on load and history is destroyed
- `raw_financial_data` is not raw. `eps_growth_rate` (a derived CAGR) is computed at fetch time (`fetcher.py`), CapEx is `abs()`-ed, and `share_cancel` is collapsed from a disclosure search into a single boolean. The original API payload is never stored, so nothing can be re-derived without re-calling DART/yfinance
- Every table is UPSERTed on `(ticker, record_date)`, where `record_date` is a *fiscal period end* (12/31, 9/30) fabricated from the DART report code — not the observation date. The weekly job overwrites the same row every run, so price/PER/ROE history is lost. Only `scoring_results` has a `score_date`
- Consequence : no point-in-time snapshot, no time-series analysis, no reproducibility, and `backtest.py` (830 lines) has to re-implement the entire fetch→process→score pipeline against live APIs just to reconstruct past years

### 2. Row grain is undefined — one row mixes four different timeframes
- A single `raw_financial_data` row contains : today's price (yfinance), annual net income (DART business report, possibly 2 fiscal years old), latest *quarterly* equity/liabilities, annual OCF/CapEx, and last calendar year's dividends. PER is therefore "today's price ÷ a year-old EPS". There is no ingestion timestamp, no source-system column, and no lineage

### 3. The transform layer is not a pure function of the warehouse
- `processor.py` calls the yfinance API *during processing* to patch ADR currency mismatches (`_get_yfinance_info_if_currency_mismatch`). The transform step is therefore non-deterministic, non-idempotent, and cannot be ported to SQL/dbt until currency is captured at ingest

### 4. The scoring model is hard-coded in Python — the core motivation for this refactor
- Thresholds live as `if per < 5: return 10` inside `scorers/v2.py`. Comparing models requires re-running `score --all` over the entire DB and writing new rows
- v1 and v2 are not even comparable (47pt vs 90pt maximums) — there is no normalized way to put two models side by side
- `score_moat` is hard-coded to `0` in `scorers/v2.py:152`. `calculate_moat_score()` is defined but **never called**, and the `qualitative_assessments` table is never read or written by any code path. 10 of the 100 points are structurally always zero, while grade thresholds (A > 80) assume a real 100-point scale
- The human-in-the-loop step is an Excel formula (`=W2+X2`) in a CSV sitting in GCS. The moat score a human enters never returns to the database, so it is lost every week

### 5. Missing data is silently indistinguishable from real zeros
- 39 `except` blocks, most swallowing to `pass` / `continue` / `0.0` defaults
- `capital_expenditure` defaults to `0.0` when the DART parse fails → FCF = OCF − 0 → inflated FCF yield → up to 10 undeserved points. `share_cancel` defaults to `False` on API error → indistinguishable from a genuine "no"
- No data quality checks, no null-rate tracking, no tests (`test_db.py` only pings the DB)
- Measured on the last 3 exports (survivors of the ≥40 filter only) : PEG null ~19%, dividend-growth-years zero ~16%

### 6. The company dimension is broken, and KR coverage has collapsed
- Every Korean company's `market` is written as the literal string `"KOSPI/KOSDAQ"` (`fetcher.py:486`) even though FinanceDataReader supplies the real exchange. US markets are stored as raw yfinance codes (`NYQ`, `NMS`) instead of NYSE/NASDAQ
- This is already a live bug : `bq_analytics._market_condition("kr")` filters on `'KOSPI','KOSDAQ'` and matches **zero rows**
- `companies` is insert-once, never updated (`if not company: add`) — no SCD, no name/market change tracking, no delisting flag. There is no sector or industry column at all, so the most natural analysis (valuation relative to sector peers) is impossible
- The pipeline fetches KR 200 / US 1000, but the last 3 weekly exports contain only **15 KR vs 246 US** stocks above the threshold — Korean data is being lost somewhere between fetch and score
- KRW and USD figures share the same columns with no currency field, so any cross-market aggregate of an absolute value (e.g. `AVG(per)` in `analyze_market_compare`) is meaningless

### 7. BigQuery is a dumping ground, not a warehouse
- `bq_export.py` writes one denormalized table (`stock_scores`), `WRITE_APPEND`, no partitioning, no clustering, no primary key, no merge
- The load is not idempotent : a re-run duplicates the week. The proof is that every query in `bq_analytics.py` opens with a `ROW_NUMBER() ... rn = 1` dedupe defending against it
- Only `scorer_version = 'v2'` is exported, so the multi-model comparison the DWH is meant to enable can't even be run in BigQuery
- Cloud SQL is the system of record and BigQuery is a lagging copy — two sources of truth

### 8. Pipeline has no orchestration, no idempotency, no retry
- `entrypoint.sh` is a linear bash script with `set -e`. A failure at step 3 leaves partial state with no retry and no per-task lineage
- `fetch_history` de-dupes by *calendar day*, so re-running after a mid-run failure skips everything already attempted and fetches nothing. A partial week cannot be repaired
- No data quality gate anywhere between fetch and export

### 9. Dead code and duplication to remove
- `backtest.py` (830 lines) — a parallel re-implementation of fetch+process+score. Becomes a SQL query over the Silver layer once history exists
- `.github/workflows/weekly_export.yml` — superseded by Cloud Run, schedule commented out
- `qualitative_assessments` model, `calculate_moat_score()` — defined, never used
- ~200 lines of terminal table-rendering (`get_display_width`, `pad_string`, column widths) in `main.py`, duplicated again in `backtest.py`. Replaced by Tableau
- The magic number `total_score >= 40` is hard-coded twice in `main.py`; grade thresholds again in `scorers/base.py`
- `main.py` is a 682-line god-module doing CLI parsing, querying, formatting, and CSV writing
