"""Tests for src/spark_session.py against a REAL local Spark + S3-compatible endpoint.

Skips cleanly if Java, Spark, or the S3 endpoint aren't available/reachable --
matching the same skip-if-unreachable pattern used in tests/test_storage.py and
tests/test_migrate_lifecycle_to_s3.py.
"""

from __future__ import annotations

import pytest

from src.spark_session import SparkSessionError, get_s3_bucket, get_spark_session, s3a_path
from src.storage import S3Storage


@pytest.fixture
def spark(spark_session, s3_storage):
    if not s3_storage.exists("raw/applications.parquet"):
        pytest.skip("raw/applications.parquet not migrated to S3 in this environment")
    return spark_session


def test_missing_java_home_raises_spark_session_error(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    with pytest.raises(SparkSessionError):
        get_spark_session("should-not-start")


def test_s3a_path_builds_expected_uri(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "data-agent")
    assert s3a_path("raw", "loans.parquet") == "s3a://data-agent/raw/loans.parquet"


def test_get_s3_bucket_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert get_s3_bucket() == "data-agent"


def test_spark_reads_migrated_parquet_from_s3(spark):
    df = spark.read.parquet(s3a_path("raw", "applications.parquet"))
    assert df.count() > 0
    assert "application_id" in df.columns


def test_spark_read_matches_pandas_row_count(spark):
    storage = S3Storage()
    pandas_df = storage.read_parquet("raw/loans.parquet")

    spark_df = spark.read.parquet(s3a_path("raw", "loans.parquet"))
    assert spark_df.count() == len(pandas_df)
