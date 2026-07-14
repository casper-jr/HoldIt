"""Ingestion package — the only code that touches an API.

Moves data, decides nothing: fetch → serialize → write. No derived values, no
defaults, no abs(), no dropped fields, no fallbacks. See docs/architecture.md.
"""
