"""GCS writer — clear the snapshot_date={ds} prefix, then write it.

Clearing the prefix before writing is load-bearing, not tidiness: object-level
overwrite alone would leave orphans if a re-run used a shorter ticker list, and
the bq load would pick them up. Wiping the prefix first makes a re-run of a date
produce exactly that date's objects and nothing else — which is what lets the
Bronze partition overwrite be idempotent.
"""
from google.cloud import storage


def prefix_for(source: str, endpoint: str, snapshot_date: str) -> str:
    """gs://{bucket}/{source}/{endpoint}/snapshot_date={ds}/"""
    return f"{source}/{endpoint}/snapshot_date={snapshot_date}/"


def clear_prefix(bucket_name: str, prefix: str) -> int:
    """Delete every object under ``prefix``. Returns the count removed."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def write_ticker(bucket_name: str, prefix: str, ticker: str, ndjson: str) -> str:
    """Write one ticker's NDJSON to {prefix}{ticker}.json. Returns the blob path."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    path = f"{prefix}{ticker}.json"
    bucket.blob(path).upload_from_string(ndjson, content_type="application/x-ndjson")
    return path
