"""Configuration — GCP Secret Manager only. No .env, no credentials in the repo.

Secrets are read lazily (function calls, not module-level fetches) so the module
imports without GCP credentials present — importing config must never require a
network call. Non-secret settings come from the environment with no default that
could mask a misconfiguration.
"""
import os

from google.cloud import secretmanager

# GCP project and the raw-landing bucket. Set in the Cloud Run Job / Airflow env.
GCP_PROJECT = os.environ.get("GCP_PROJECT")
RAW_BUCKET = os.environ.get("HOLDIT_RAW_BUCKET", "holdit-raw")

# Secret Manager secret ids (the resource name, not the value).
DART_API_KEY_SECRET = os.environ.get("DART_API_KEY_SECRET", "dart-api-key")


def get_secret(secret_id: str, version: str = "latest") -> str:
    """Return the payload of a Secret Manager secret as a string."""
    if not GCP_PROJECT:
        raise RuntimeError("GCP_PROJECT is not set; cannot resolve secrets")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def dart_api_key() -> str:
    """The DART Open API key. Read at call time, never cached in the repo."""
    return get_secret(DART_API_KEY_SECRET)
