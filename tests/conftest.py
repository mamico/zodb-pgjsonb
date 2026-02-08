"""Shared test configuration for zodb-pgjsonb tests."""

import os


# Allow DSN override via environment variable for CI.
# Default: local Docker on port 5433 (development setup).
DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)
