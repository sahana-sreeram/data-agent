# Generalizability: a unified context layer for agent action on RHOAI

This project is a full, concrete instance of a lending data platform -- but the two
architectural boundaries it's built on are domain-agnostic by construction, and both have
already been proven swappable in practice, not just in theory: the exact same code ran
unmodified against two independently-provisioned OpenShift clusters (a shared RHOAI cluster
and a personal ROSA cluster) this session, with only environment-specific config (images,
credentials, namespaces) differing between them.

## 1. The compute/runtime boundary: `src/platform_backends/`

Three small `typing.Protocol` interfaces, each with a real local implementation and a real
RHOAI-backed implementation coexisting behind the same call sites:

- **`PipelineRunner`** (`pipeline_runner.py`): `submit`/`get_status`/`await_completion`.
  `LocalSparkRunner` runs `local[*]` Spark in-process; `RHOAISparkRunner` submits a real
  `SparkApplication` custom resource to the Spark Operator.
- **`RuntimeInspector`** (`runtime_inspector.py`): `get_run_summary`/`get_failed_stages`/
  `get_driver_log_excerpt`/`get_pod_status`. `LocalRuntimeInspector` uses PySpark's own
  `SparkContext.statusTracker()`; `SparkHistoryRuntimeInspector` calls the real Spark History
  Server REST API and the Kubernetes API for pod/log data.
- **`StateStore`** (`state_store.py`): `get`/`set`/`delete`/`list_keys`. `FileStateStore` wraps
  S3-compatible object storage; `RedisStateStore` is a real, minimal Redis-backed alternative.

None of the agent, diagnosis, repair, or MCP-tool code cares which implementation is behind
these interfaces -- `src/config.py`'s factories pick one from an env var. Swapping Spark for a
different compute engine (RHOAI's own Kubeflow-based Data Science Pipelines, dbt, Flink,
anything else) means implementing these same three small interfaces once, not touching the
agent.

## 2. The context boundary: the MCP data-ops tool schema

The more important, more directly reusable contract is `src/mcp_servers/data_ops_server.py`'s
tool schema itself:

- `get_data_product_context` -- what is this data product, where does it come from
- `get_metric_context` -- a specific metric's formula, source fields, and approval status
- `get_lineage` -- the chain from a business metric back to its upstream source(s)
- `get_runtime_health` -- is this data product currently trustworthy right now
- `get_relevant_pipeline_code` -- the actual transformation logic that produced it
- `run_data_product_validation` -- an independent, deterministic trust check, separate from
  whether the job merely executed without error

**This is the "unified context layer" the wider framing question is really asking about.**
Any RHOAI-hosted data domain -- not just lending, not just Spark -- that implements these six
read functions against its own metadata gets the exact same safe, grounded diagnose -> propose
-> verify loop this project already proves, with any MCP-speaking agent (Codex or otherwise)
as the consumer, regardless of what compute engine is underneath. The domain-specific part
(`src/context_retriever.py`'s `ContextRetriever`, and the lending-specific `context/*.json`
files it reads) is a real *implementation* of this contract, not the contract itself.

This session's own context-layer ablation demo (see `../../demo/DEMO_SCRIPT.md` Act 5,
`src/context_retriever.py::BlindContextRetriever`) makes this concrete: strip these six tools
down to nulls and the same real model, given the same real failing pipeline, measurably loses
the ability to explain *why* something is wrong or *where* to fix it -- even with raw code and
raw configuration still in hand. That's the actual value proposition of building this contract
explicitly, rather than assuming a capable-enough model doesn't need it.

## What "generalizable" would mean concretely, as follow-on work

Everything above is already structurally generalizable (the swap points exist and are proven).
Making the *claim* fully concrete -- beyond this one lending example -- would mean:

1. Writing the six-tool contract up as a standalone spec, independent of `context_retriever.py`'s
   lending-specific implementation.
2. Plugging a second, structurally different data domain into the same MCP schema (even a small
   one) to prove it's not secretly lending-shaped.
3. Adding a second `PipelineRunner` implementation targeting RHOAI's own Data Science
   Pipelines (Kubeflow-based), alongside `RHOAISparkRunner`, as the natural "second compute
   engine" proof point for an RHOAI-native audience.

None of this is required for the current demo to make its point -- the swap points already
exist, are already tested, and were already proven live across two real clusters today.
