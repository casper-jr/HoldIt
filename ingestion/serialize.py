"""Serialization — the only place a fetched object becomes JSON.

Serializing is mechanical and allowed; deriving is not. These functions turn a
pandas/dict payload into a JSON-embeddable structure and wrap it in the Bronze
record shape. They compute no values, drop no fields, and supply no defaults.

DataFrames and Series use ``to_json(orient='split')`` because it preserves the
index — and for the price history and financial statements, the index labels
*are* the dates and fiscal period ends. ``orient='records'`` would drop them.
"""
import datetime
import json

import pandas as pd


def to_payload(obj):
    """Normalize a fetched object into a JSON-embeddable value.

    - DataFrame / Series -> the parsed ``orient='split'`` object (columns, index,
      data), so ``payload`` nests as real JSON rather than a stringified blob and
      stays queryable with JSON_VALUE / JSON_QUERY in Silver.
    - dict whose values are DataFrames/Series (e.g. the three financial
      statements) -> each value normalized, keys untouched.
    - anything else (e.g. ``ticker.info``) -> returned as-is.
    """
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        return json.loads(obj.to_json(orient="split", date_format="iso"))
    if isinstance(obj, dict):
        return {key: to_payload(value) for key, value in obj.items()}
    return obj


def build_record(
    *,
    snapshot_date: str,
    source: str,
    endpoint: str,
    ticker: str,
    request_params: dict,
    http_status,
    payload,
    ingested_at: datetime.datetime,
) -> dict:
    """Build one Bronze row. ``payload`` is None on a failed fetch; nothing is
    defaulted to zero. ``http_status`` is None when the source exposes no HTTP
    status (yfinance is a library, not a raw HTTP call)."""
    return {
        "snapshot_date": snapshot_date,
        "ingested_at": ingested_at.isoformat(),
        "source": source,
        "endpoint": endpoint,
        "ticker": ticker,
        "request_params": request_params,
        "http_status": http_status,
        "payload": to_payload(payload) if payload is not None else None,
    }


def to_ndjson(record: dict) -> str:
    """One Bronze record -> one NDJSON line. ``allow_nan=False`` makes a
    non-JSON value (NaN/Inf) fail here rather than land as invalid JSON."""
    return json.dumps(record, allow_nan=False) + "\n"
