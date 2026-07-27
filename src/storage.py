"""S3-compatible storage abstraction (local dev target: MinIO).

Credentials and endpoint are read from the environment (loaded here from a
.env file at the project root, if present, via python-dotenv) -- never
hardcoded, never logged. Only the endpoint URL and bucket name have local-dev
defaults (matching the `data-agent-minio` container); the access key and
secret have no default and raise clearly if unset, same discipline as
OPENAI_API_KEY in src/model_client.py.

Implements exactly the interface from claude_code_handoff_mvp_to_demo.md's
Phase 3: read_json/write_json/read_parquet/write_parquet/exists/list_paths/
copy_or_promote. Parquet I/O goes through an in-memory buffer -- no local
temp files, no s3fs dependency, just boto3 + pandas + pyarrow (already
project dependencies).

This module does not decide what gets migrated or when -- see
src/migrate_lifecycle_to_s3.py for that.
"""

from __future__ import annotations

import io
import json
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

DEFAULT_ENDPOINT_URL = "http://localhost:9000"
DEFAULT_BUCKET = "data-agent"
DEFAULT_REGION = "us-east-1"


class StorageError(Exception):
    """Raised for storage configuration or operation failures."""


class S3Storage:
    """A thin, explicit wrapper around boto3 for one bucket.

    Every path is a key relative to the bucket root (e.g. "raw/loans.parquet"),
    never a full s3:// URI -- the bucket itself is fixed per instance.
    """

    def __init__(
        self,
        bucket: str | None = None,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        self.bucket = bucket or os.environ.get("S3_BUCKET", DEFAULT_BUCKET)
        endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL", DEFAULT_ENDPOINT_URL)
        access_key_id = access_key_id or os.environ.get("S3_ACCESS_KEY_ID")
        secret_access_key = secret_access_key or os.environ.get("S3_SECRET_ACCESS_KEY")

        if not access_key_id or not secret_access_key:
            raise StorageError(
                "S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY are not set. Copy .env.example to .env "
                "at the project root and fill them in, or export them in your shell."
            )

        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=DEFAULT_REGION,
        )

    def create_bucket_if_missing(self) -> bool:
        """Create the bucket if it doesn't exist yet. Returns True if it was just created."""
        existing = {b["Name"] for b in self._client.list_buckets()["Buckets"]}
        if self.bucket in existing:
            return False
        self._client.create_bucket(Bucket=self.bucket)
        return True

    def exists(self, path: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def list_paths(self, prefix: str) -> list[str]:
        paths: list[str] = []
        continuation_token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self._client.list_objects_v2(**kwargs)
            paths.extend(obj["Key"] for obj in response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            continuation_token = response["NextContinuationToken"]
        return sorted(paths)

    def read_json(self, path: str):
        response = self._client.get_object(Bucket=self.bucket, Key=path)
        return json.loads(response["Body"].read().decode("utf-8"))

    def write_json(self, path: str, value) -> None:
        body = json.dumps(value, indent=2).encode("utf-8")
        self._client.put_object(Bucket=self.bucket, Key=path, Body=body, ContentType="application/json")

    def read_parquet(self, path: str) -> pd.DataFrame:
        response = self._client.get_object(Bucket=self.bucket, Key=path)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer, engine="pyarrow")

    def write_parquet(self, path: str, dataframe: pd.DataFrame) -> None:
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, engine="pyarrow", index=False)
        self._client.put_object(
            Bucket=self.bucket, Key=path, Body=buffer.getvalue(), ContentType="application/octet-stream"
        )

    def delete(self, path: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=path)

    def copy_or_promote(self, source: str, destination: str) -> None:
        """Server-side copy within the same bucket -- no download/re-upload.

        This is the S3 analog of src/verify_repair.py's local shutil.copy2 promotion:
        moving a verified candidate object into its curated/promoted location.
        """
        self._client.copy_object(
            Bucket=self.bucket,
            Key=destination,
            CopySource={"Bucket": self.bucket, "Key": source},
        )
