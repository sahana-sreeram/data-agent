"""Shared PySpark session factory, pre-configured to read/write the same
S3-compatible bucket src/storage.py talks to (local dev: MinIO).

JAVA_HOME and the S3_* credentials are read from the environment (loaded here
from .env via python-dotenv, same discipline as src/storage.py and
src/model_client.py -- never hardcoded, never logged). JAVA_HOME must be set
BEFORE pyspark's JVM gateway launches, which is why it's applied to
os.environ here rather than left to the caller's shell.

The hadoop-aws + AWS SDK jars needed for s3a:// are pulled from Maven Central
on first use via spark.jars.packages (cached under ~/.ivy2 afterwards -- the
first run in a fresh environment will be slow, ~1 minute; subsequent runs are
fast). No manual JAR download or SPARK_HOME setup required.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"


class SparkSessionError(Exception):
    """Raised when required environment configuration is missing."""


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SparkSessionError(
            f"{name} is not set. Copy .env.example to .env at the project root and fill it in."
        )
    return value


def get_spark_session(app_name: str) -> SparkSession:
    """Build (or return the active) local SparkSession, configured for s3a://<bucket>/...

    Callers read/write via paths like f"s3a://{bucket}/raw/loans.parquet" -- get the
    bucket name from get_s3_bucket() below rather than hardcoding "data-agent".
    """
    java_home = _require_env("JAVA_HOME")
    os.environ["JAVA_HOME"] = java_home

    endpoint_url = _require_env("S3_ENDPOINT_URL")
    access_key = _require_env("S3_ACCESS_KEY_ID")
    secret_key = _require_env("S3_SECRET_ACCESS_KEY")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", HADOOP_AWS_PACKAGE)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def get_s3_bucket() -> str:
    """Same default as src/storage.py's S3Storage -- "data-agent" if S3_BUCKET is unset."""
    return os.environ.get("S3_BUCKET", "data-agent")


def s3a_path(*parts: str) -> str:
    """Build an s3a:// URI: s3a_path("raw", "loans.parquet") -> s3a://<bucket>/raw/loans.parquet."""
    bucket = os.environ.get("S3_BUCKET", "data-agent")
    return f"s3a://{bucket}/" + "/".join(parts)
