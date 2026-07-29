"""Real HTTP implementation of runtime_inspector.HistoryServerClient, talking to a real Spark
History Server's REST API (confirmed live against one, 2026-07-28). Uses urllib (stdlib) rather
than adding a new HTTP-client dependency -- this is a handful of simple GET-JSON calls.

get_executor_log is honest about a real limitation, confirmed live: a completed run's
executors report an empty `executorLogs` map once their pods are gone (History Server only
serves executor logs through a live executor's own web UI proxy, which no longer exists after
the job finishes) -- there is no real log CONTENT to fetch via this API for a finished run.
Real driver/executor log text on RHOAI comes from the pod's own logs (kubectl/oc logs, or the
Kubernetes API) while the pod still exists, not from History Server at all.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request


class HistoryServerClientError(Exception):
    """Raised for a real HTTP/parsing failure talking to the History Server."""


class RealHistoryServerClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _get_json(self, path: str):
        url = f"{self._base_url}/api/v1{path}"
        try:
            with urllib.request.urlopen(url, timeout=self._timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise HistoryServerClientError(f"request to {url!r} failed: {exc}") from exc

    def get_application(self, run_id: str) -> dict:
        return self._get_json(f"/applications/{run_id}")

    def get_stages(self, run_id: str) -> list[dict]:
        return self._get_json(f"/applications/{run_id}/stages")

    def get_executor_log(self, run_id: str, executor_id: str, log_type: str) -> str:
        executors = self._get_json(f"/applications/{run_id}/executors")
        matching = next((e for e in executors if e.get("id") == executor_id), None)
        log_url = (matching or {}).get("executorLogs", {}).get(log_type)
        if not log_url:
            return (
                f"(no {log_type} log available via Spark History Server for executor {executor_id!r} -- "
                "its pod's own logs, e.g. via `oc logs`, are needed while the pod still exists; "
                "History Server only proxies a live executor's log, not a finished one's)"
            )
        try:
            with urllib.request.urlopen(log_url, timeout=self._timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError) as exc:
            raise HistoryServerClientError(f"request to {log_url!r} failed: {exc}") from exc
