"""Pluggable workflow-state backends -- resumable state shared between MCP tool calls (e.g.
`create_candidate_repair` persists a diagnosis+repair-plan record that a later, separate
`verify_candidate_repair` call reads back to finish the job).

FileStateStore formalizes a pattern this codebase already uses ad hoc: `S3Storage.read_json`/
`write_json`/`exists` against keys like `curated/pipeline_run.json` and
`curated/pending_repairs/<pipeline>.json` (see src/data_ops.py's `_pending_repair_key`). It
namespaces everything under a `state/` prefix so it never collides with those existing keys,
and is the default everywhere STATE_BACKEND is unset.

RedisStateStore is a real, minimal alternative for when many concurrent MCP tool calls need
lower-latency shared state than round-tripping through S3 -- optional, lazy-imported, and
skipped cleanly in tests when no local Redis is reachable (mirrors tests/conftest.py's
`_s3_reachable()` skip pattern).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from src.storage import S3Storage

STATE_KEY_PREFIX = "state/"


class StateStore(Protocol):
    def get(self, key: str) -> dict | None: ...

    def set(self, key: str, value: dict) -> None: ...

    def delete(self, key: str) -> None: ...

    def list_keys(self, prefix: str = "") -> list[str]: ...


@dataclass
class FileStateStore:
    """Default backend -- wraps S3Storage under a `state/` key prefix. `storage` defaults to
    a real `S3Storage()` if not given, matching every other storage-backed class in this
    codebase (e.g. `LocalSparkRunner`)."""

    storage: S3Storage = field(default_factory=S3Storage)

    def _full_key(self, key: str) -> str:
        return f"{STATE_KEY_PREFIX}{key}.json"

    def get(self, key: str) -> dict | None:
        full_key = self._full_key(key)
        if not self.storage.exists(full_key):
            return None
        return self.storage.read_json(full_key)

    def set(self, key: str, value: dict) -> None:
        self.storage.write_json(self._full_key(key), value)

    def delete(self, key: str) -> None:
        full_key = self._full_key(key)
        if self.storage.exists(full_key):
            self.storage.delete(full_key)

    def list_keys(self, prefix: str = "") -> list[str]:
        full_prefix = f"{STATE_KEY_PREFIX}{prefix}"
        paths = self.storage.list_paths(full_prefix)
        stripped = [p.removeprefix(STATE_KEY_PREFIX).removesuffix(".json") for p in paths]
        return stripped


def _default_redis_client(url: str) -> Any:
    """Imported lazily -- `redis` is an optional dependency (see pyproject.toml's `rhoai`
    extra); local-only usage never needs it installed."""
    import redis

    return redis.Redis.from_url(url, decode_responses=True)


@dataclass
class RedisStateStore:
    """`client` is injected for testing (any object matching redis-py's `get`/`set`/`delete`/
    `keys` shape); production code passes `url` and leaves `client` unset -- a real client is
    built lazily on first use."""

    url: str = "redis://localhost:6379/0"
    client: Any = field(default=None)

    def _redis(self) -> Any:
        if self.client is None:
            self.client = _default_redis_client(self.url)
        return self.client

    def _full_key(self, key: str) -> str:
        return f"{STATE_KEY_PREFIX}{key}"

    def get(self, key: str) -> dict | None:
        raw = self._redis().get(self._full_key(key))
        return json.loads(raw) if raw is not None else None

    def set(self, key: str, value: dict) -> None:
        self._redis().set(self._full_key(key), json.dumps(value))

    def delete(self, key: str) -> None:
        self._redis().delete(self._full_key(key))

    def list_keys(self, prefix: str = "") -> list[str]:
        pattern = f"{STATE_KEY_PREFIX}{prefix}*"
        raw_keys = self._redis().keys(pattern)
        return [k.removeprefix(STATE_KEY_PREFIX) for k in raw_keys]
