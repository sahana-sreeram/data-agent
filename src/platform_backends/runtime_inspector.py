"""Pluggable runtime-evidence backends: what actually happened when a pipeline ran, as
distinct from ContextRetriever's `get_runtime_health` fact (which reports the last known
health *status*, not execution detail like stages/tasks/logs).

LocalRuntimeInspector reads PySpark's own in-process `SparkContext.statusTracker()` --
real, live data, no separate History Server process needed -- scoped to one run via the job
group `LocalSparkRunner.submit()` tags every job with (see pipeline_runner.py). This is what
makes the "live Spark evidence" story demoable locally today, not just after RHOAI access.

SparkHistoryRuntimeInspector calls a real Spark History Server's REST API instead, via an
injected `HistoryServerClient`-shaped object (for testing) or a real `requests`-based one
built lazily on first use.

Selected by src.config.get_runtime_inspector() based on RUNTIME_BACKEND (local|spark_history).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

DEFAULT_LOG_LINES = 100
MAX_LOG_LINES = 500


class RuntimeInspector(Protocol):
    def get_run_summary(self, run_id: str) -> dict: ...

    def get_failed_stages(self, run_id: str) -> list[dict]: ...

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = DEFAULT_LOG_LINES) -> str: ...

    def get_pod_status(self, run_id: str) -> dict: ...


@dataclass
class LocalRuntimeInspector:
    """Reads the active local SparkSession's statusTracker for jobs/stages tagged with
    run_id as their job group. `spark` defaults to the shared local session
    (`get_spark_session`) if not given -- the same `getOrCreate()`-shared JVM every other
    local caller in this codebase already uses."""

    spark: Any = None

    def _tracker(self):
        if self.spark is None:
            from src.spark_session import get_spark_session

            self.spark = get_spark_session("runtime-inspector")
        return self.spark.sparkContext.statusTracker()

    def get_run_summary(self, run_id: str) -> dict:
        tracker = self._tracker()
        job_ids = list(tracker.getJobIdsForGroup(run_id))
        jobs = [tracker.getJobInfo(job_id) for job_id in job_ids]
        jobs = [j for j in jobs if j is not None]
        stage_ids = sorted({stage_id for job in jobs for stage_id in job.stageIds})
        return {
            "run_id": run_id,
            "job_count": len(jobs),
            "stage_count": len(stage_ids),
            "job_statuses": [job.status for job in jobs],
            "overall_status": "SUCCEEDED" if jobs and all(j.status == "SUCCEEDED" for j in jobs) else ("FAILED" if any(j.status == "FAILED" for j in jobs) else "UNKNOWN"),
        }

    def get_failed_stages(self, run_id: str) -> list[dict]:
        tracker = self._tracker()
        job_ids = list(tracker.getJobIdsForGroup(run_id))
        failed = []
        for job_id in job_ids:
            job = tracker.getJobInfo(job_id)
            if job is None:
                continue
            for stage_id in job.stageIds:
                stage = tracker.getStageInfo(stage_id)
                if stage is not None and stage.numFailedTasks > 0:
                    failed.append({"stage_id": stage_id, "name": stage.name, "num_failed_tasks": stage.numFailedTasks})
        return failed

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = DEFAULT_LOG_LINES) -> str:
        # Local mode has no separate driver log file distinct from this process's own
        # stdout/stderr -- there is deliberately nothing more specific to return here than a
        # note explaining that. Real driver logs are a RHOAI/SparkHistoryRuntimeInspector
        # (or OpenShift pod-log) concept; see that implementation below.
        return "(local mode: no separate driver log -- this process's own stdout/stderr is the driver log)"

    def get_pod_status(self, run_id: str) -> dict:
        return {"available": False, "reason": "local mode has no pods"}


class HistoryServerClient(Protocol):
    """The minimal Spark History Server REST surface this inspector needs -- injected for
    tests; a real implementation is a thin `requests`-based wrapper around
    `<history-server>/api/v1/applications/<app_id>/...`, built lazily, never at import time."""

    def get_application(self, run_id: str) -> dict: ...

    def get_stages(self, run_id: str) -> list[dict]: ...

    def get_executor_log(self, run_id: str, executor_id: str, log_type: str) -> str: ...


def _default_history_server_client(base_url: str) -> HistoryServerClient:
    from src.platform_backends._history_server_http_client import RealHistoryServerClient

    return RealHistoryServerClient(base_url)


def _default_k8s_core_v1_client() -> Any:
    """Constructed only when SparkHistoryRuntimeInspector's pod-status/log methods are used
    with no injected client -- mirrors pipeline_runner._default_k8s_client's in-cluster-first,
    kubeconfig-fallback pattern exactly. `kubernetes` (an optional dependency, see
    pyproject.toml's `rhoai` extra) is imported here, never at module load time."""
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()


@dataclass
class SparkHistoryRuntimeInspector:
    """Wraps a real Spark History Server's REST API for job/stage evidence (`client`, any
    object matching the `HistoryServerClient` Protocol -- injected for testing; production
    code passes `base_url` and leaves it unset, a real HTTP client is built lazily). Pod
    status and driver logs, though, are NOT History Server concepts at all -- confirmed live
    that a finished run's `executorLogs` are empty once its pods are gone, since History
    Server only proxies a *live* executor's own log endpoint. Those two methods instead query
    the Kubernetes API directly (`k8s_client`, any object matching the
    `kubernetes.client.CoreV1Api` shape -- injected for testing; built lazily otherwise)."""

    base_url: str = "http://spark-history-server:18080"
    client: HistoryServerClient | None = field(default=None)
    truncate_at: int = MAX_LOG_LINES
    namespace: str = "data-agent"
    k8s_client: Any = field(default=None)

    def _http(self) -> HistoryServerClient:
        if self.client is None:
            self.client = _default_history_server_client(self.base_url)
        return self.client

    def _k8s(self) -> Any:
        if self.k8s_client is None:
            self.k8s_client = _default_k8s_core_v1_client()
        return self.k8s_client

    def get_run_summary(self, run_id: str) -> dict:
        app = self._http().get_application(run_id)
        return {
            "run_id": run_id,
            "overall_status": app.get("attempts", [{}])[-1].get("completed") and "SUCCEEDED" or "RUNNING_OR_FAILED",
            "raw": app,
        }

    def get_failed_stages(self, run_id: str) -> list[dict]:
        stages = self._http().get_stages(run_id)
        return [s for s in stages if s.get("status") == "FAILED"]

    def get_driver_log_excerpt(self, pod_name: str, max_lines: int = DEFAULT_LOG_LINES) -> str:
        """pod_name -- NOT a run_id or Spark application id; callers (see
        src.mcp_servers.spark_runtime_server.SparkRuntimeTools._driver_pod_name) resolve the
        actual k8s pod name from the RunHandle first, since that's the only id this evidence
        source understands."""
        bounded_max_lines = min(max_lines, self.truncate_at)
        try:
            # Confirmed live: the kubernetes client's default (_preload_content=True) response
            # deserialization for this endpoint is broken -- it returns a `str`, but produced
            # via `str(raw_bytes)` internally rather than `raw_bytes.decode(...)`, so the
            # content literally starts with the two characters `b` and `'` (Python's bytes
            # repr syntax), not the real log text. Passing _preload_content=False bypasses
            # that broken path entirely and gives the raw urllib3 response to decode ourselves.
            response = self._k8s().read_namespaced_pod_log(
                name=pod_name, namespace=self.namespace, tail_lines=bounded_max_lines, _preload_content=False
            )
        except Exception as exc:  # noqa: BLE001 -- a real k8s/log-read failure is reportable evidence, not a crash
            return f"(could not read pod log for {pod_name!r}: {exc})"
        return response.data.decode("utf-8", errors="replace")

    def get_pod_status(self, pod_name: str) -> dict:
        """pod_name -- see get_driver_log_excerpt's docstring."""
        try:
            pod = self._k8s().read_namespaced_pod(name=pod_name, namespace=self.namespace)
        except Exception as exc:  # noqa: BLE001 -- a real k8s read failure is reportable evidence, not a crash
            return {"available": False, "reason": str(exc)}
        container_statuses = [
            {"name": c.name, "ready": c.ready, "restart_count": c.restart_count}
            for c in (pod.status.container_statuses or [])
        ]
        return {"available": True, "pod_name": pod_name, "phase": pod.status.phase, "container_statuses": container_statuses}
