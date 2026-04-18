# HoldIt

A quantitative stock screening and scoring tool for long-term investors, inspired by Warren Buffett and Peter Lynch's investment philosophy. HoldIt automates the collection, processing, and scoring of financial data for both Korean (KRX) and US (NYSE/NASDAQ) listed stocks.

---

## Overview

HoldIt helps identify undervalued, dividend-paying quality stocks by:

1. **Fetching** raw financial data from DART API (KR) and yfinance (US)
2. **Processing** raw data into evaluation metrics (PER, ROE, FCF yield, PEG, etc.)
3. **Scoring** each stock on a 100-point scale across three categories
4. **Viewing** a ranked leaderboard filtered by market and scorer version
5. **Exporting** results to CSV for manual qualitative review in Excel

The scoring model is designed to filter out value traps (low PER but no growth) by incorporating ROE, Free Cash Flow, and PEG alongside traditional valuation metrics.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11+ |
| Database | PostgreSQL (via Docker) |
| ORM | SQLAlchemy 2.0 |
| KR Data Source | [DART Open API](https://opendart.fss.or.kr) + FinanceDataReader |
| US Data Source | yfinance (screener, financials, dividends) |
| Containerization | Docker Compose |

---

## Scoring System (v2 — Default)

Total score is **100 points**, divided into three categories.

### Category 1 — Profitability & Intrinsic Value (40pts)

| Metric | Max Score | Notes |
|---|---|---|
| PER | 10 | < 5 → 10pts, < 8 → 7pts, < 10 → 5pts, < 15 → 2pts, ≥ 15 → 0pts |
| ROE | 15 | > 20% → 15pts, > 15% → 10pts, > 10% → 5pts (Buffett: sustained high ROE signals moat) |
| FCF Yield | 10 | > 8% → 10pts, > 5% → 7pts, > 3% → 4pts, > 0% → 2pts |
| PBR | 5 | ROE-linked adjustment: high-ROE firms justify a premium above book value |

### Category 2 — Growth & Financial Safety (30pts)

| Metric | Max Score | Notes |
|---|---|---|
| Economic Moat | 10 | *Qualitative* — pricing power, market share, brand (user-entered) |
| PEG Ratio | 10 | < 0.5 → 10pts, < 1.0 → 7pts, < 1.5 → 4pts, ≥ 1.5 → 0pts (Lynch: PEG < 1 is fair value) |
| Debt Ratio | 10 | < 30% → 10pts, < 60% → 7pts, < 100% → 4pts, < 200% → 2pts |

### Category 3 — Shareholder Return Policy (30pts)

| Metric | Max Score | Notes |
|---|---|---|
| Dividend Yield | 10 | > 7% → 10pts, > 5% → 7pts, > 3% → 5pts, < 3% → 2pts |
| Dividend Growth | 10 | ≥ 10 consecutive years → 10pts, ≥ 7yrs → 8pts, ≥ 5yrs → 6pts, ≥ 3yrs → 3pts |
| Share Cancellation | 10 | Confirmed buyback cancellation in past year → 10pts (cancellation only, not mere repurchase) |

### Investment Grade

| Score | Grade | Action |
|---|---|---|
| > 80 | A | Strong buy candidate |
| 70–80 | B | Buy consideration |
| 50–70 | C | Hold if already owned |
| < 50 | D | Not recommended |

> Stocks that cannot reach grade C (50pts) even with a perfect qualitative score (10pts moat) are automatically excluded from the leaderboard and exports. This means any stock with a quantitative score below 40 is filtered out.

---

## Multi-Scorer Architecture

The scorer is versioned and swappable via a factory pattern, allowing comparison between different evaluation frameworks without affecting existing data.

| Version | Description | Max Quantitative Score |
|---|---|---|
| `v1` | Legacy — PER/PBR/dividend focused | 47pts |
| `v2` | Current — Full Buffett & Lynch model | 90pts (+ 10pts qualitative) |

- Each version is stored independently in the `scoring_results` table via a `scorer_version` column.
- To add a new version: create `scorers/vN.py` inheriting `ScorerBase`, then register it in `scorers/__init__.py`'s `_SCORERS` dict.

---

## Data Pipeline

```
[1] fetch    →  raw_financial_data        (DART API / yfinance)
[2] process  →  processed_financial_data  (EPS, PER, ROE, FCF, PBR, PEG, debt ratio, dividend yield)
[3] score    →  scoring_results           (per-metric scores + total + grade)
[4] view     →  terminal leaderboard      (ranked by score, filterable by market/scorer)
[5] export   →  export/*.csv              (Excel-compatible with formula for qualitative input)
```

---

## Database Schema

| Table | Purpose |
|---|---|
| `companies` | Master list of tickers with name and market |
| `raw_financial_data` | Raw figures from APIs (price, net income, equity, OCF, CapEx, dividends, etc.) |
| `processed_financial_data` | Derived metrics (EPS, PER, ROE, FCF yield, PBR, PEG, debt ratio, dividend yield) |
| `qualitative_assessments` | User-entered qualitative ratings (economic moat, management quality) |
| `scoring_results` | Final per-metric scores, total score, grade, and scorer version |
| `fetch_history` | Per-ticker daily fetch status to prevent duplicate collection |

---

## Data Sources

### Korean Stocks (KR)
- **Stock list**: `FinanceDataReader.StockListing('KRX')` sorted by market cap
- **Financials**: DART `fnlttSinglAcnt.json` — annual business report (most recent fiscal year)
- **Balance sheet**: DART `fnlttSinglAcntAll.json` — most recent quarterly filing
- **Dividends**: DART `alotMatter.json` — up to 12 years of dividend history
- **Share cancellation**: DART disclosure search for '소각' keyword
- **Current price**: yfinance

### US Stocks (US)
- **Stock list**: yfinance screener (`yf.screen()`) — NYSE + NASDAQ sorted by market cap; preferred shares (`-P` pattern) excluded
- **Financials**: yfinance `quarterly_income_stmt` — trailing 12 months (TTM, last 4 quarters)
- **EPS growth rate**: yfinance `income_stmt` annual EPS CAGR (up to 3 years), falling back to `earningsGrowth`
- **Balance sheet**: yfinance `quarterly_balance_sheet`
- **Cash flow**: yfinance `cashflow` (Operating Cash Flow, CapEx)
- **Dividends**: yfinance annual dividend history
- **ADR currency mismatch handling**: For tickers where price currency ≠ financial currency, `trailingPE` and `priceToBook` from `yf.info` are used directly

---

## Backtesting

`backtest.py` reconstructs historical scores for any ticker without modifying the database.

```bash
# Score AAPL for the last 5 fiscal years + current DB snapshot
python3 backtest.py AAPL 5

# Specify exact years
python3 backtest.py AAPL 2022 2023 2024

# Korean stock
python3 backtest.py 005930 5

# Compare with legacy scorer
python3 backtest.py AAPL 5 --scorer v1
```

Output is a year-by-year comparison table covering all metrics and per-category scores.

---

## Setup

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- [DART API key](https://opendart.fss.or.kr/intro/main.do) (for Korean stocks)

### 1. Clone and install dependencies

```bash
git clone https://github.com/casper-jr/HoldIt.git
cd HoldIt
pip install -r requirements.txt
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
DART_API_KEY=your_dart_api_key_here
DB_URL=postgresql://user:password@localhost:5432/holdit
```

### 3. Start the database

```bash
docker-compose up -d
```

---

## Usage

```bash
# [Step 1] Fetch raw data
python3 main.py fetch kr 100          # Top 100 Korean stocks by market cap
python3 main.py fetch kr all          # All KRX-listed stocks
python3 main.py fetch us 50           # Top 50 US stocks by market cap

# Force re-fetch a specific ticker (ignores today's fetch history)
python3 main.py refetch 005930        # Korean stock
python3 main.py refetch AAPL          # US stock

# [Step 2] Process raw data into evaluation metrics
python3 main.py process               # Today's fetched data only
python3 main.py process --all         # Reprocess entire database

# [Step 3] Score all processed stocks
python3 main.py score                 # Today's processed data only (v2 default)
python3 main.py score --all           # Re-score entire database
python3 main.py score --all --scorer v1   # Score with legacy v1

# [Step 4] View leaderboard (C grade or above, accounting for max qualitative score)
python3 main.py view                  # All markets, v2 scorer
python3 main.py view kr               # Korean stocks only
python3 main.py view us 20            # Top 20 US stocks
python3 main.py view kr --scorer v1   # Korean stocks, v1 scorer

# View detailed breakdown for a single stock
python3 main.py detail 005930         # Korean stock
python3 main.py detail KO             # US stock

# [Step 5] Export to CSV for qualitative evaluation in Excel
python3 main.py export                # All markets
python3 main.py export kr             # Korean stocks only
python3 main.py export us             # US stocks only
```

The exported CSV includes:
- Raw metric values and per-metric scores
- A blank column for manual economic moat scoring (0–10)
- An Excel formula column that auto-calculates the final total score

---

## Project Structure

```
holdit/
├── main.py              # CLI entry point — fetch, process, score, view, export
├── fetcher.py           # DartFetcher (KR) and USFetcher (US) — API data collection
├── processor.py         # FinancialProcessor — computes derived metrics from raw data
├── scorers/             # Scorer package (factory pattern, version-swappable)
│   ├── __init__.py      #   get_scorer(version) factory + StockScorer alias
│   ├── base.py          #   ScorerBase — shared score_all(), get_grade(), _save()
│   ├── v1.py            #   ScorerV1 — legacy (PER/PBR/dividend, max 47pts)
│   └── v2.py            #   ScorerV2 — Buffett & Lynch model (max 100pts) [default]
├── backtest.py          # Year-by-year historical score reconstruction
├── models.py            # SQLAlchemy ORM models
├── database.py          # DB session and engine setup
├── config.py            # Environment variable loading
├── docker-compose.yml   # PostgreSQL container configuration
├── requirements.txt     # Python dependencies
├── scoring_guidelines.md # Scoring rubric with investment philosophy rationale (Korean)
└── export/              # CSV exports for qualitative review
```
