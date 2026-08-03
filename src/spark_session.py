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

import glob
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

_FALLBACK_HADOOP_VERSION = "3.4.2"


def _detect_hadoop_version() -> str:
    """hadoop-aws MUST exactly match the Hadoop client version pyspark itself bundles
    (hadoop-client-api/runtime-<version>.jar, always present in pyspark's own jars/
    directory) -- a mismatch causes real, confusing runtime failures deep in S3A
    initialization (confirmed live: pyspark 4.2.0 bundles Hadoop 3.5.0, and hardcoding
    hadoop-aws:3.4.2 against it raised `NoClassDefFoundError:
    software/amazon/s3/analyticsaccelerator/request/ObjectClient` -- a Hadoop-3.5-only S3A
    input-stream feature hadoop-aws 3.4.2 doesn't declare, so its ivy-resolved dependency
    closure doesn't include it). Different pyspark releases end up installed in different
    environments (confirmed live: 4.1.2 locally vs. 4.2.0 in the deployed image, apparently
    due to available wheels differing by Python version) -- detecting the version actually
    present, rather than hardcoding one, means this is correct regardless of which pyspark
    release is installed anywhere, present or future."""
    import pyspark

    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    matches = glob.glob(os.path.join(jars_dir, "hadoop-client-api-*.jar"))
    if not matches:
        return _FALLBACK_HADOOP_VERSION
    filename = os.path.basename(matches[0])
    return filename.removeprefix("hadoop-client-api-").removesuffix(".jar")


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

    builder = SparkSession.builder.appName(app_name)
    # SPARK_APPLICATION_ID is set by spark-submit itself, only in a process it actually
    # launched as a driver under the Spark Operator's cluster mode -- confirmed live in a real
    # driver pod's env. That's a more precise signal than EXECUTION_BACKEND=rhoai (src/config.py):
    # EXECUTION_BACKEND=rhoai is also set on plain Deployments in the same namespace (e.g. the
    # data-ops MCP server), which call get_spark_session() to rerun a repair's ETL LOCALLY
    # inside their own pod, exactly like they always have -- they are never spark-submit
    # drivers themselves, so they must still get .master("local[*]"). Only skip it when
    # spark-submit already passed a real k8s master URL, or this would silently override that
    # and run everything in-process, never scheduling real executor pods.
    if "SPARK_APPLICATION_ID" not in os.environ:
        builder = builder.master("local[*]")

    return (
        builder
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{_detect_hadoop_version()}")
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
