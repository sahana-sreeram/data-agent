"""Pluggable pipeline-execution backends.

LocalSparkRunner reproduces exactly what src/run_lifecycle_etl_pipelines.py already does --
it reuses that module's own `PIPELINES` registry of per-pipeline (etl_fn, validate_fn)
closures against one local SparkSession -- byte for byte, so no existing call site changes
behavior by this class existing. RHOAISparkRunner submits a SparkApplication custom resource
to a Spark-Operator-managed cluster instead, via an injected Kubernetes client (never
constructed eagerly, never required for local-only usage -- the `kubernetes` package is only
imported inside `_default_k8s_client()`, and only when no client was injected).

Selected by src.config.get_pipeline_runner() based on EXECUTION_BACKEND (local|rhoai),
mirroring how src.lifecycle_run_self_healing already picks a SandboxBackend
(src/sandbox/backend.py) via a plain conditional rather than a registry/DI framework.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.spark_session import get_spark_session
from src.storage import S3Storage

DEFAULT_NAMESPACE = "data-agent"  # the real RHOAI project this demo deploys into (see deploy/rhoai/)
SPARK_OPERATOR_GROUP = "sparkoperator.k8s.io"
SPARK_OPERATOR_VERSION = "v1beta2"
SPARK_APPLICATION_PLURAL = "sparkapplications"


@dataclass(frozen=True)
class RunHandle:
    """Opaque reference to one pipeline run -- callers pass this back into get_status/
    await_completion/RuntimeInspector calls without needing to know which backend produced
    it. run_id is always a real, locally-generated id (both backends); backend_ref is
    backend-specific (None for local; the SparkApplication's namespace/name for RHOAI)."""

    pipeline_name: str
    run_id: str
    backend: str  # "local" | "rhoai"
    backend_ref: dict | None = None


class PipelineRunner(Protocol):
    def submit(self, pipeline_name: str) -> RunHandle: ...

    def get_status(self, handle: RunHandle) -> dict: ...

    def await_completion(self, handle: RunHandle, timeout_seconds: float = 300.0) -> dict: ...


class LocalSparkRunner:
    """Default backend -- byte-for-byte today's existing behavior: reuses
    src.run_lifecycle_etl_pipelines.PIPELINES' own (etl_fn, validate_fn) closures for the
    named pipeline against one local SparkSession, exactly like that module's own
    `run_all_pipelines` does for every pipeline. submit() is synchronous (local Spark has no
    separate "running" state to poll) -- get_status/await_completion both just return the
    already-complete result."""

    def __init__(self, storage: S3Storage | None = None) -> None:
        self._storage = storage or S3Storage()
        self._results: dict[str, dict] = {}

    def submit(self, pipeline_name: str) -> RunHandle:
        from src.run_lifecycle_etl_pipelines import PIPELINES

        if pipeline_name not in PIPELINES:
            raise ValueError(f"unknown pipeline {pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")

        run_id = uuid.uuid4().hex[:12]
        etl_fn, validate_fn = PIPELINES[pipeline_name]
        business_rules = self._storage.read_json("context/business_rules.json")
        spark = get_spark_session(f"pipeline-runner-{pipeline_name}")
        spark.sparkContext.setLogLevel("WARN")
        # Tags every Spark job this run submits with run_id as its job group -- this is what
        # lets LocalRuntimeInspector (src/platform_backends/runtime_inspector.py) scope
        # statusTracker() queries to exactly this run instead of the whole shared local JVM.
        spark.sparkContext.setJobGroup(run_id, f"pipeline-runner:{pipeline_name}")

        try:
            etl_fn(spark, self._storage, business_rules)
        except Exception as exc:  # noqa: BLE001 -- a run failure is a reportable status, not a crash
            self._results[run_id] = {"status": "FAILED", "etl_status": "FAILURE", "validation": None, "error": str(exc)}
            return RunHandle(pipeline_name=pipeline_name, run_id=run_id, backend="local")

        validation = validate_fn(self._storage, business_rules)
        self._results[run_id] = {
            "status": "SUCCEEDED",
            "etl_status": "SUCCESS",
            "validation": validation,
            "error": None,
        }
        return RunHandle(pipeline_name=pipeline_name, run_id=run_id, backend="local")

    def get_status(self, handle: RunHandle) -> dict:
        return self._results.get(handle.run_id, {"status": "UNKNOWN"})

    def await_completion(self, handle: RunHandle, timeout_seconds: float = 300.0) -> dict:
        return self.get_status(handle)  # already complete by the time submit() returns


def _default_k8s_client() -> Any:
    """Constructed only when RHOAISparkRunner is used with no injected client -- the
    `kubernetes` package (an optional dependency, see pyproject.toml's `rhoai` extra) is
    imported here, never at module load time, so local-only usage never needs it installed.

    Tries in-cluster config first (the mounted service account token/CA cert -- what every
    real deployment, e.g. the spark-runtime MCP server pod, actually needs) and falls back to
    the local kubeconfig (~/.kube/config, via `oc`/`kubectl` login) for running this against a
    real cluster from a developer's own machine outside a pod."""
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
    return kubernetes.client.CustomObjectsApi()


@dataclass
class RHOAISparkRunner:
    """Submits a SparkApplication custom resource (the Spark Operator's CRD) instead of
    running Spark in-process. `k8s_client` is injected for testing -- any object exposing
    `create_namespaced_custom_object`/`get_namespaced_custom_object` (the
    `kubernetes.client.CustomObjectsApi` shape) works; production code leaves it unset and
    `_default_k8s_client()` builds a real one lazily on first use."""

    namespace: str = DEFAULT_NAMESPACE
    # A DIFFERENT image from the console/MCP servers' -- must be built FROM an official Apache
    # Spark image (e.g. deploy/rhoai/Dockerfile.spark), never the bare python:3.12-slim image
    # deploy/rhoai/Dockerfile builds for the console/MCP servers. Confirmed live: the Spark
    # Operator's driver/executor containers rely on the image's own ENTRYPOINT being a real
    # Spark k8s bootstrap script -- it only ever APPENDS args like `driver --properties-file
    # ...` to whatever the image's entrypoint already is, never overrides it.
    image: str = "REPLACE_WITH_BUILT_SPARK_IMAGE"  # set post-cluster-access; see deploy/rhoai/
    secret_name: str = "data-agent-secrets"
    s3_endpoint_url: str = "http://minio:9000"
    k8s_client: Any = field(default=None)

    def _client(self) -> Any:
        if self.k8s_client is None:
            self.k8s_client = _default_k8s_client()
        return self.k8s_client

    def _manifest(self, pipeline_name: str, app_name: str) -> dict:
        spec = PIPELINE_REGISTRY[pipeline_name]
        return {
            "apiVersion": f"{SPARK_OPERATOR_GROUP}/{SPARK_OPERATOR_VERSION}",
            "kind": "SparkApplication",
            "metadata": {"name": app_name, "namespace": self.namespace, "labels": {"pipeline": pipeline_name}},
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": self.image,
                "mainApplicationFile": f"local:///opt/spark-app/{spec.etl_source_file}",
                "sparkVersion": "3.5.5",
                # Deliberately NOT using spec.deps.packages -- confirmed live that the Spark
                # Operator's own controller pod has a non-writable Ivy cache, so Maven package
                # resolution fails before a driver pod is even created. hadoop-aws + its AWS SDK
                # (v1 -- matches deploy/rhoai/Dockerfile.spark's base image's bundled Hadoop
                # 3.3.4 client) are baked into the image's jars/ directory at build time instead
                # -- mirrors sparkapplication-loan-portfolio.yaml exactly.
                "sparkConf": {
                    # Mirrors src/spark_session.py's real S3a config exactly -- see
                    # deploy/rhoai/configmap-spark-defaults.yaml for the ConfigMap this maps to.
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.dir": "s3a://data-agent/spark-events/",
                    # Confirmed live: this Spark Operator build silently drops
                    # spec.driver.env/envFrom entirely (a known issue -- see
                    # kubeflow/spark-operator#1108). Using Spark's OWN native
                    # (operator-independent) env-injection config keys instead, which
                    # spark-submit itself honors when building the pod spec.
                    "spark.kubernetes.driver.secretKeyRef.S3_ACCESS_KEY_ID": f"{self.secret_name}:S3_ACCESS_KEY_ID",
                    "spark.kubernetes.driver.secretKeyRef.S3_SECRET_ACCESS_KEY": f"{self.secret_name}:S3_SECRET_ACCESS_KEY",
                    "spark.kubernetes.executor.secretKeyRef.S3_ACCESS_KEY_ID": f"{self.secret_name}:S3_ACCESS_KEY_ID",
                    "spark.kubernetes.executor.secretKeyRef.S3_SECRET_ACCESS_KEY": f"{self.secret_name}:S3_SECRET_ACCESS_KEY",
                    "spark.kubernetes.driverEnv.S3_ENDPOINT_URL": self.s3_endpoint_url,
                    "spark.kubernetes.driverEnv.EXECUTION_BACKEND": "rhoai",
                    "spark.executorEnv.S3_ENDPOINT_URL": self.s3_endpoint_url,
                    "spark.executorEnv.EXECUTION_BACKEND": "rhoai",
                },
                "restartPolicy": {"type": "Never"},
                # serviceAccount/cores/coreLimit/memory match
                # deploy/rhoai/sparkapplication-loan-portfolio.yaml exactly -- a Spark-on-k8s
                # driver needs its own namespace-scoped service account to manage the executor
                # pods/service it creates (see deploy/rhoai/role.yaml). coreLimit is required
                # because this namespace's LimitRange auto-injects a default 500m CPU limit on
                # any container that doesn't set one explicitly, which a bare cores:1 (a full
                # core REQUEST) then exceeds -- confirmed live, the driver pod is rejected at
                # admission before it's ever created without this.
                "driver": {"serviceAccount": "data-agent-app", "cores": 1, "coreLimit": "1", "memory": "1g", "labels": {"pipeline": pipeline_name}},
                "executor": {"instances": 1, "cores": 1, "coreLimit": "1", "memory": "1g", "labels": {"pipeline": pipeline_name}},
            },
        }

    def submit(self, pipeline_name: str) -> RunHandle:
        if pipeline_name not in PIPELINE_REGISTRY:
            raise ValueError(f"unknown pipeline {pipeline_name!r}; known: {sorted(PIPELINE_REGISTRY)}")
        run_id = uuid.uuid4().hex[:12]
        app_name = f"{pipeline_name.replace('_', '-')}-{run_id}"
        manifest = self._manifest(pipeline_name, app_name)
        self._client().create_namespaced_custom_object(
            group=SPARK_OPERATOR_GROUP,
            version=SPARK_OPERATOR_VERSION,
            namespace=self.namespace,
            plural=SPARK_APPLICATION_PLURAL,
            body=manifest,
        )
        return RunHandle(
            pipeline_name=pipeline_name,
            run_id=run_id,
            backend="rhoai",
            backend_ref={"namespace": self.namespace, "name": app_name},
        )

    def get_status(self, handle: RunHandle) -> dict:
        obj = self._client().get_namespaced_custom_object(
            group=SPARK_OPERATOR_GROUP,
            version=SPARK_OPERATOR_VERSION,
            namespace=handle.backend_ref["namespace"],
            plural=SPARK_APPLICATION_PLURAL,
            name=handle.backend_ref["name"],
        )
        app_state = (obj.get("status") or {}).get("applicationState", {}).get("state", "UNKNOWN")
        return {"status": app_state, "raw": obj.get("status")}

    def await_completion(self, handle: RunHandle, timeout_seconds: float = 300.0) -> dict:
        import time

        deadline = time.monotonic() + timeout_seconds
        status = self.get_status(handle)
        while status["status"] not in ("COMPLETED", "FAILED") and time.monotonic() < deadline:
            time.sleep(2.0)
            status = self.get_status(handle)
        return status
