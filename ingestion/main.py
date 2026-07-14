"""Ingestion entrypoint — fetch one (source, endpoint) for a snapshot date.

CLI: --source --endpoint --snapshot-date [--limit] [--start --end].

The flow is fetch -> serialize -> write and nothing else. A per-ticker failure is
recorded as a Bronze row with a null payload and the run continues; one bad ticker
never kills the run. The prefix is cleared before writing so a re-run of a date is
idempotent. The weekly-vs-backfill date policy for price_history is Step 3; here
the range is passed explicitly via --start/--end.

Only the US (yf) path is wired. KR (dart) needs the corp_code resolution and KR
universe that land in Step 6.
"""
import argparse
import datetime

from ingestion import gcs_writer, serialize, universe
from ingestion.config import RAW_BUCKET
from ingestion.sources import yfinance_client


def run_yf(endpoint: str, snapshot_date: str, limit: int, start: str, end: str) -> None:
    if endpoint == "price_history" and not (start and end):
        raise SystemExit("price_history requires --start and --end")

    tickers = universe.us_tickers(limit)
    prefix = gcs_writer.prefix_for("yf", endpoint, snapshot_date)
    gcs_writer.clear_prefix(RAW_BUCKET, prefix)

    for ticker in tickers:
        ingested_at = datetime.datetime.now(datetime.timezone.utc)
        try:
            payload, request_params = yfinance_client.fetch(
                endpoint, ticker, start=start, end=end
            )
            http_status = None  # yfinance is a library, not a raw HTTP call
        except Exception:
            # Per-ticker failure: record a null payload, keep going.
            payload, request_params, http_status = None, {}, None

        record = serialize.build_record(
            snapshot_date=snapshot_date,
            source="yf",
            endpoint=endpoint,
            ticker=ticker,
            request_params=request_params,
            http_status=http_status,
            payload=payload,
            ingested_at=ingested_at,
        )
        gcs_writer.write_ticker(
            RAW_BUCKET, prefix, ticker, serialize.to_ndjson(record)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="HoldIt ingestion")
    parser.add_argument("--source", required=True, choices=["yf", "dart"])
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    if args.source == "yf":
        run_yf(args.endpoint, args.snapshot_date, args.limit, args.start, args.end)
    else:
        raise NotImplementedError("dart ingestion is wired in Step 6")


if __name__ == "__main__":
    main()
