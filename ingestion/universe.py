"""Ticker universe — which tickers a run fetches.

The architecture tree did not give the universe a home; this module is that home.
Selection derives no *data* — it only decides which tickers to request — so it
belongs in ingestion. The US screener is ported here from the old main.py; the KR
universe (KRX listing intersected with DART corp codes) lands in Step 6.
"""
import yfinance as yf


def us_tickers(limit: int) -> list[str]:
    """US common stocks by descending market cap, preferred shares excluded.

    Ported from the old main.py: yfinance's screener over NASDAQ (NMS) and NYSE
    (NYQ), market-cap ranked, dropping ``-P`` preferred tickers the way major
    indices do. Paginates at 250/page.
    """
    query = yf.EquityQuery(
        "AND",
        [
            yf.EquityQuery(
                "OR",
                [
                    yf.EquityQuery("eq", ["exchange", "NMS"]),
                    yf.EquityQuery("eq", ["exchange", "NYQ"]),
                ],
            ),
            yf.EquityQuery("gt", ["intradaymarketcap", 0]),
        ],
    )

    symbols: list[str] = []
    for offset in range(0, limit, 250):
        size = min(250, limit - offset)
        result = yf.screen(
            query,
            sortField="intradaymarketcap",
            sortAsc=False,
            size=size,
            offset=offset,
        )
        quotes = result.get("quotes", [])
        symbols.extend(q.get("symbol", "") for q in quotes)
        if len(quotes) < size:
            break

    return [s for s in symbols if s and "-P" not in s][:limit]


def kr_tickers(limit: int) -> list[str]:
    """KR universe — KRX listing ∩ DART corp codes. Added in Step 6."""
    raise NotImplementedError("KR universe is Step 6")
