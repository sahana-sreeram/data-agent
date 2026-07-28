"""Environment-driven selection of platform backends (src.platform_backends) and the agent
harness (src.agents). Every factory's default branch reproduces today's exact existing
behavior -- an unset environment is indistinguishable from the system before this module
existed. Mirrors the plain-conditional backend-selection style already used by
src.lifecycle_run_self_healing for its SandboxBackend choice (src/sandbox/backend.py) rather
than a registry or DI framework.
"""

from __future__ import annotations

import os

EXECUTION_BACKEND_ENV_VAR = "EXECUTION_BACKEND"  # "local" (default) | "rhoai"
RUNTIME_BACKEND_ENV_VAR = "RUNTIME_BACKEND"  # "local" (default) | "spark_history"
STATE_BACKEND_ENV_VAR = "STATE_BACKEND"  # "file" (default) | "redis"
AGENT_HARNESS_ENV_VAR = "AGENT_HARNESS"  # "current" (default) | "codex_mcp"


def execution_backend() -> str:
    return os.environ.get(EXECUTION_BACKEND_ENV_VAR, "local")


def runtime_backend() -> str:
    return os.environ.get(RUNTIME_BACKEND_ENV_VAR, "local")


def state_backend() -> str:
    return os.environ.get(STATE_BACKEND_ENV_VAR, "file")


def agent_harness() -> str:
    return os.environ.get(AGENT_HARNESS_ENV_VAR, "current")


def get_pipeline_runner():
    backend = execution_backend()
    if backend == "rhoai":
        from src.platform_backends.pipeline_runner import RHOAISparkRunner

        return RHOAISparkRunner(namespace=os.environ.get("RHOAI_NAMESPACE", "data-agent"))
    if backend != "local":
        raise ValueError(f"unknown {EXECUTION_BACKEND_ENV_VAR}={backend!r}; expected 'local' or 'rhoai'")

    from src.platform_backends.pipeline_runner import LocalSparkRunner

    return LocalSparkRunner()


def get_runtime_inspector():
    backend = runtime_backend()
    if backend == "spark_history":
        from src.platform_backends.runtime_inspector import SparkHistoryRuntimeInspector

        return SparkHistoryRuntimeInspector(base_url=os.environ.get("SPARK_HISTORY_SERVER_URL", "http://spark-history-server:18080"))
    if backend != "local":
        raise ValueError(f"unknown {RUNTIME_BACKEND_ENV_VAR}={backend!r}; expected 'local' or 'spark_history'")

    from src.platform_backends.runtime_inspector import LocalRuntimeInspector

    return LocalRuntimeInspector()


def get_state_store():
    backend = state_backend()
    if backend == "redis":
        from src.platform_backends.state_store import RedisStateStore

        return RedisStateStore(url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    if backend != "file":
        raise ValueError(f"unknown {STATE_BACKEND_ENV_VAR}={backend!r}; expected 'file' or 'redis'")

    from src.platform_backends.state_store import FileStateStore

    return FileStateStore()
