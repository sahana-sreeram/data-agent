"""Shared fixtures for tests that need a real local S3-compatible endpoint (MinIO)
and/or a real local Spark session. Session-scoped so the ~10s Spark JVM startup and
S3 connectivity check happen once per test run, not once per test module.

All tests using these fixtures skip cleanly (rather than fail) when the
infrastructure isn't available in the current environment.
"""

from __future__ import annotations

import pytest

from src.spark_session import SparkSessionError, get_spark_session
from src.storage import S3Storage, StorageError


def _s3_reachable() -> bool:
    try:
        storage = S3Storage()
        storage._client.list_buckets()
        return True
    except (StorageError, Exception):  # noqa: BLE001 -- any connectivity/config failure
        return False


@pytest.fixture(scope="session")
def s3_storage() -> S3Storage:
    if not _s3_reachable():
        pytest.skip("S3-compatible storage not reachable")
    return S3Storage()


@pytest.fixture(scope="session")
def spark_session(s3_storage):
    try:
        session = get_spark_session("lifecycle-etl-tests")
    except SparkSessionError as exc:
        pytest.skip(str(exc))
    except Exception as exc:  # noqa: BLE001 -- e.g. no Java on PATH
        pytest.skip(f"Spark could not start: {exc}")
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


class PrefixedStorage:
    """Thin wrapper redirecting read/write calls to a test-prefixed key, so a
    validate_*.py module's fixed "raw/..."/"curated/..." keys resolve to isolated
    test fixtures instead of real migrated data. Shared by every
    tests/test_etl_spark_*.py module -- previously five near-identical, slowly
    diverging copies of this same class.
    """

    def __init__(self, storage, prefix: str) -> None:
        self._storage = storage
        self._prefix = prefix
        self.bucket = storage.bucket

    def read_parquet(self, path: str):
        return self._storage.read_parquet(f"{self._prefix}{path}")

    def write_parquet(self, path: str, dataframe) -> None:
        self._storage.write_parquet(f"{self._prefix}{path}", dataframe)

    def read_json(self, path: str):
        return self._storage.read_json(f"{self._prefix}{path}")

    def write_json(self, path: str, value) -> None:
        self._storage.write_json(f"{self._prefix}{path}", value)

    def exists(self, path: str) -> bool:
        return self._storage.exists(f"{self._prefix}{path}")
