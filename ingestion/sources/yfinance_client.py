"""yfinance client — fetch, return the payload as-is.

yfinance's own output is what counts as "raw" here (Yahoo's HTTP response is not
exposed by the library). ``ticker.info`` is a dict; the statement and price calls
return DataFrames whose index carries the dates. Nothing here casts, fills, drops,
or computes — the returned object is handed straight to ``serialize``.

There is no HTTP status to report: yfinance is a library, not a raw HTTP call, so
``http_status`` is None for every yf endpoint and a failed fetch is signalled by a
null payload upstream (see ``ingestion.main``).
"""
import yfinance as yf

# Endpoints map 1:1 to Bronze tables raw_yf_{endpoint}.
ENDPOINTS = ("quote", "price_history", "financials", "dividends")


def fetch(endpoint: str, ticker: str, *, start: str = None, end: str = None):
    """Fetch one endpoint for one ticker. Returns (payload_obj, request_params).

    payload_obj is a raw dict / DataFrame / Series / dict-of-DataFrames. The
    caller serializes it; this function decides nothing about its contents.
    """
    t = yf.Ticker(ticker)

    if endpoint == "quote":
        return t.info, {}

    if endpoint == "price_history":
        # auto_adjust=False keeps both Close and Adj Close: adjusted close is
        # rewritten by every split/dividend, so a PER from it is wrong. Bronze
        # keeps both; Silver decides which to use.
        df = t.history(start=start, end=end, auto_adjust=False)
        return df, {"start": start, "end": end, "auto_adjust": False}

    if endpoint == "financials":
        # Three statements in one payload; Silver's stg_yf__financials parses it.
        return (
            {
                "income_stmt": t.income_stmt,
                "balance_sheet": t.balance_sheet,
                "cashflow": t.cashflow,
            },
            {},
        )

    if endpoint == "dividends":
        return t.dividends, {}

    raise ValueError(f"unknown yf endpoint: {endpoint!r}")
