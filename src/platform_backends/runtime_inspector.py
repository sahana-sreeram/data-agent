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


@dataclass
class SparkHistoryRuntimeInspector:
    """Wraps a real Spark History Server's REST API. `client` is injected for testing (any
    object matching the `HistoryServerClient` Protocol); production code passes `base_url`
    and leaves `client` unset -- a real HTTP client is built lazily on first use."""

    base_url: str = "http://spark-history-server:18080"
    client: HistoryServerClient | None = field(default=None)
    truncate_at: int = MAX_LOG_LINES

    def _http(self) -> HistoryServerClient:
        if self.client is None:
            self.client = _default_history_server_client(self.base_url)
        return self.client

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

    def get_driver_log_excerpt(self, run_id: str, max_lines: int = DEFAULT_LOG_LINES) -> str:
        max_lines = min(max_lines, self.truncate_at)
        log_text = self._http().get_executor_log(run_id, executor_id="driver", log_type="stdout")
        lines = log_text.splitlines()
        return "\n".join(lines[-max_lines:])

    def get_pod_status(self, run_id: str) -> dict:
        # Pod status is an OpenShift/Kubernetes concept, not a History Server one -- callers
        # needing this against RHOAI should query the cluster directly (see
        # src.platform_backends.pipeline_runner.RHOAISparkRunner.get_status), not this class.
        return {"available": False, "reason": "pod status is not exposed by Spark History Server"}
