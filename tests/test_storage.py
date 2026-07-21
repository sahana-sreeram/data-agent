"""Tests for src/storage.py against a REAL local S3-compatible endpoint (MinIO).

Skips cleanly if no reachable endpoint/credentials are configured -- this module
intentionally does not mock boto3, matching this codebase's existing preference for
exercising real behavior over mocks (see tests/test_verify_repair.py's docstring).
All test writes go under a dedicated _test_storage/ prefix, cleaned up before and
after, so they never collide with real migrated data under raw/ or context/.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.storage import S3Storage, StorageError

TEST_PREFIX = "_test_storage/"


def _cleanup(storage: S3Storage) -> None:
    for key in storage.list_paths(TEST_PREFIX):
        storage._client.delete_object(Bucket=storage.bucket, Key=key)


@pytest.fixture
def storage():
    try:
        s = S3Storage()
        s.create_bucket_if_missing()
        s._client.list_buckets()
    except Exception as exc:  # noqa: BLE001 -- any connectivity/config failure means skip
        pytest.skip(f"S3-compatible storage not reachable: {exc}")
    _cleanup(s)
    yield s
    _cleanup(s)


def test_missing_credentials_raises_storage_error(monkeypatch):
    monkeypatch.delenv("S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(StorageError):
        S3Storage()


def test_write_and_read_json_round_trips(storage):
    path = f"{TEST_PREFIX}hello.json"
    value = {"a": 1, "b": None, "c": ["x", "y"]}
    storage.write_json(path, value)
    assert storage.read_json(path) == value


def test_write_and_read_parquet_round_trips_including_nulls(storage):
    path = f"{TEST_PREFIX}df.parquet"
    df = pd.DataFrame(
        {
            "id": pd.array(["A1", None, "A3"], dtype="string"),
            "amount": [10.5, 20.0, None],
        }
    )
    storage.write_parquet(path, df)
    result = storage.read_parquet(path)
    assert result["id"].tolist() == ["A1", pd.NA, "A3"]
    assert result["amount"].isna().sum() == 1
    assert len(result) == 3


def test_exists_true_and_false(storage):
    path = f"{TEST_PREFIX}exists_check.json"
    assert storage.exists(path) is False
    storage.write_json(path, {"present": True})
    assert storage.exists(path) is True


def test_list_paths_filters_by_prefix(storage):
    storage.write_json(f"{TEST_PREFIX}group_a/one.json", {"n": 1})
    storage.write_json(f"{TEST_PREFIX}group_a/two.json", {"n": 2})
    storage.write_json(f"{TEST_PREFIX}group_b/three.json", {"n": 3})

    group_a_paths = storage.list_paths(f"{TEST_PREFIX}group_a/")
    assert group_a_paths == [f"{TEST_PREFIX}group_a/one.json", f"{TEST_PREFIX}group_a/two.json"]

    all_paths = storage.list_paths(TEST_PREFIX)
    assert len(all_paths) == 3


def test_copy_or_promote_creates_new_object_with_same_content(storage):
    source = f"{TEST_PREFIX}source.json"
    destination = f"{TEST_PREFIX}promoted/destination.json"
    value = {"promoted": True, "run_id": "abc123"}
    storage.write_json(source, value)

    storage.copy_or_promote(source, destination)

    assert storage.exists(destination) is True
    assert storage.read_json(destination) == value
    # The source is untouched by a copy (unlike a move).
    assert storage.exists(source) is True
