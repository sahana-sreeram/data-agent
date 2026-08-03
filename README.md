# data-agent

An agentic data-operations control layer for a synthetic lending company's data estate — not
a chatbot over a table. 6 independently versioned upstream services emit domain events into
S3-compatible object storage; 6 PySpark ETL pipelines curate them into business metrics; a
context-enrichment layer automatically derives most of the technical documentation (schemas,
joins, lineage) from the code and runtime itself, leaving only genuine business-semantics
decisions to a human; and the system walks a full incident lifecycle — business signal → trust
check → incident → diagnosis → governed repair → sandboxed verification → reviewable PR
artifact — whenever the data behind a question can't be trusted. Natural-language Q&A is one
entry point into that lifecycle, not the whole of it. The context layer — not the agent loop —
is the differentiator: the incident-response behavior is the proof that the enriched context
is actually useful, not the product itself.

## How it works

- **Ingest** — 6 upstream services (`services/`) each own a slice of the lending lifecycle
  (marketing, applications, underwriting, loans, payments, risk) and emit versioned domain
  events; `src/events_to_lifecycle_tables.py` projects them into curated-pipeline-ready raw
  tables. `src/generate_upstream_events.py` scales this to hundreds of thousands of customers
  in bounded-memory batches — verified end-to-end at 20,000 customers / 8.65M raw rows (see
  "At-scale verification" below).
- **Transform** — 6 PySpark pipelines produce curated metrics: loan portfolio, campaign
  funnel, underwriting performance, payment performance, delinquency/default, coupon
  performance (onboarded purely via manifest, see "Onboard" below).
- **Validate** — an independent pandas validator recomputes each metric from raw data and
  reconciles it against the ETL output; a separate 12-table raw validator catches upstream
  contract changes reconciliation structurally cannot (see `src/eval_scenarios.py`'s
  `UpstreamContractScenario`, and the flagship demo below).
- **Enrich** — `src/context_enrichment/` derives dataset schemas, ETL joins/filters/business-
  rule references, and lineage (including which upstream service produces each raw table) from
  code and runtime state; a minimal `context/human/*.yaml` layer holds only what a human must
  decide (canonical metric definitions, approval, repair policy). `src/context_store/` merges
  both with an explicit precedence order, surfacing any mismatch as evidence rather than
  picking a winner.
- **Operate** — `src/data_ops.py` and the console UI (`static/`) walk the full business-signal
  → trust-check → incident → diagnosis → repair → verify → PR-artifact lifecycle, narrated.
  `ask_lifecycle.py` (`/api/ask`) answers a single question directly when that's all that's
  needed — one entry point into the same system, not a separate product.
- **Self-heal** — if a data product can't be trusted, the agent diagnoses why, has an LLM
  propose a repair, verifies it against an isolated sandbox rerun (`src/sandbox/`), and either
  promotes it directly or produces a local, reviewable PR artifact (`src/pr_artifact.py`)
  instead — never both. A normally-refused root-cause category (e.g. an upstream contract
  change) can be explicitly, narrowly unlocked by an operator for one incident at a time — see
  the flagship demo.
- **Onboard** — new pipelines/services register via a manifest (`pipelines/*.yaml`,
  `src/manifest_loader.py`) rather than core-code changes. Note: the natural-language Q&A tool
  surface (`src/lifecycle_business_tools.py`) is still hand-authored per dataset — onboarding a
  pipeline gets it a governed ETL/validate/repair lifecycle for free, not automatic Q&A.
- **Evaluate** — `eval_harness.py` injects real ETL bugs and a genuine upstream contract
  change; `src/eval_report.py` buckets results (and real demo runs) into four categories that
  are never merged into one number — see "Evaluations" below.

## Quickstart

Prerequisites: Docker, a JDK for PySpark (e.g. `brew install openjdk@17`), and an
OpenAI API key.

```
cp .env.example .env   # fill in OPENAI_API_KEY and JAVA_HOME
./scripts/bootstrap.sh # starts MinIO, installs deps, generates + migrates + runs ETL
python3 -m src.api     # http://127.0.0.1:8000 -- the console (Overview/Q&A/Run Details tabs)
```

`bootstrap.sh` is idempotent — safe to rerun any time (e.g. after regenerating data). For the
event-sourced/scale path instead of one-shot generation, see `services/README.md`.

## Flagship demo: an upstream contract change, not a code bug

`src/demo/enterprise_incident.py` walks a deterministic, real (not simulated) scenario:
`payment_service` ships a v2 contract renaming a successfully-collected installment's status
from `PAID` to `SETTLED`. Event generation, ingestion, and the Spark ETL all report success —
the data product is still silently wrong, caught only by independent validation.

```
python3 -m src.demo.enterprise_incident --healthy-only            # Stage 3: trusted baseline
python3 -m src.demo.enterprise_incident --inject-contract-change   # Stage 4: deploy the contract change
python3 -m src.demo.enterprise_incident --run-repair               # Stages 5-11: investigate, refuse,
                                                                    #   human-approve, repair, verify, PR
python3 -m src.demo.enterprise_incident --reset                    # restore everything, idempotent
```

Default (`--scripted-model`) costs no API calls — the real diagnose/repair/verify agent loops
run against real S3/Spark, only the model's responses are canned, replaying the exact
tool-call sequence a real run of this scenario produces. `--live-model` makes real OpenAI
calls instead. Every run persists a manifest to `curated/demo_runs/` (S3) and
`demo_output/run_manifest.json` (local).

What actually happens, live-verified end to end:
1. **Deploy**: raw contract validation fails (`payment_events_payment_status_enum_valid`);
   business reconciliation fails independently (`total_outstanding_principal_status_
   vocabulary_drift`) — two different validators, two different mechanisms, same real incident.
2. **Investigate** (no approval): diagnosis correctly names `SOURCE_CONTRACT_CHANGE`, traces
   the lineage chain to `payment_service`, and the repair is **refused** by default policy —
   a contract change is never auto-repaired.
3. **Human-approved repair**: an operator explicitly approves a candidate for this one
   incident. The repair targets a narrowly-scoped, pipeline-owned config pointer
   (`context/pipeline_rules/loan_portfolio.json`) — never the shared, cross-pipeline
   `context/business_rules.json`, never the ETL source directly — repointing it at an
   already-approved ruleset (`context/business_rules_demo.json`). Applied in a real
   git worktree, Spark reruns against the isolated candidate, independent validation passes,
   and the outcome is `VERIFIED_PENDING_PR` with a real PR-ready artifact (diff, branch, risk
   classification, human-review flag) — never auto-promoted.

## At-scale verification

Verified for real at 20,000 customers (not simulated or projected): 8.65M raw rows across 12
tables, 410MB/2,636 files of upstream events, all 6 ETL pipelines and their validators passing
(including the drift check above, confirming its threshold is ratio-based and holds at scale).
`python3 -m src.data_ops scale` reports the current estate's measured (never hardcoded)
customer/row/file/byte counts.

```
python3 -m src.generate_upstream_events --profile demo --output s3 --seed 42
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```

Known limitation: the flagship demo's injection scenario reconstructs a fixed 300-customer
payment subset regardless of current scale, so running it against a 20K-customer estate
produces mismatched loan_ids and silently defeats the reconciliation check. The live
environment in this repo is kept at the original small scale so the demo stays correct; the
20K proof above is a one-time, documented verification, not the permanent environment state.

## RHOAI platform mode (optional)

Everything above runs entirely locally (`local[*]` Spark, MinIO, direct Python calls) and is
the default -- nothing below changes that path. `src/platform_backends/` additionally makes the
same lifecycle runnable as a real platform capability on Red Hat OpenShift AI: Spark submitted
via the Spark Operator, runtime evidence pulled from Spark History Server/OpenShift, and Codex
driving the whole loop through MCP tool calls instead of direct Python calls.

Four env vars (all default to the local behavior above; see `.env.example`):

| Var | Values | Selects |
|---|---|---|
| `EXECUTION_BACKEND` | `local` (default) \| `rhoai` | `LocalSparkRunner` vs `RHOAISparkRunner` (submits a `SparkApplication` CR) |
| `RUNTIME_BACKEND` | `local` (default) \| `spark_history` | `LocalRuntimeInspector` (PySpark `statusTracker()`) vs `SparkHistoryRuntimeInspector` (real History Server REST API) |
| `AGENT_HARNESS` | `current` (default) \| `codex_mcp` | `src.data_ops`'s direct Python calls vs `src.agents.codex_mcp_loop` (Codex driving everything through MCP) |
| `STATE_BACKEND` | `file` (default) \| `redis` | `FileStateStore` (S3-backed) vs `RedisStateStore` |

Install the extra dependencies these RHOAI-backed implementations need (never required for the
local path): `pip install -e ".[rhoai]"`.

Two MCP servers expose this system's existing capabilities as tools instead of Python calls:

- **`src/mcp_servers/spark_runtime_server.py`** ("Spark Runtime MCP"): `submit_spark_pipeline`,
  `get_spark_application_status`, `get_spark_run_summary`, `get_failed_stages`,
  `get_driver_log_excerpt` (bounded -- never sends a full raw log), `get_pod_status`.
- **`src/mcp_servers/data_ops_server.py`** ("Data Operations MCP"): `get_data_product_context`,
  `get_metric_context`, `get_lineage`, `get_runtime_health`, `get_relevant_pipeline_code`,
  `run_data_product_validation`, `create_candidate_repair`, `verify_candidate_repair`,
  `get_pr_ready_artifact`.

`src/agents/codex_mcp_loop.py` is the `AGENT_HARNESS=codex_mcp` "morning data-operations loop":
Codex connects to both servers as a real MCP client (not a direct Python call, even locally --
see the module docstring for how) and drives a bounded tool-calling loop across every pipeline
it's given, exactly mirroring `src.data_ops.auto_scan_and_repair`'s outcome (`VERIFIED_PENDING_PR`,
never an automatic promotion) but reached through genuine MCP round trips.

```
python3 -m src.agents.codex_mcp_loop --pipeline loan_portfolio   # real OpenAI call
```

The console's Run Details tab (`GET /api/run-details/{run_id|latest}`) shows a `codex_mcp` run's
own tool-call timeline and final report alongside the same data-product estate and pending-repair
review package it always showed -- it works identically whether or not a Codex/MCP run has ever
happened.

`deploy/rhoai/` has the Kubernetes/OpenShift manifests (Spark Operator `SparkApplication`,
Spark History Server, both MCP servers, minimal namespace-scoped RBAC, ConfigMaps/Secret
templates -- no real secrets committed), `deploy/rhoai/RUNBOOK.md` for deploying them,
`deploy/rhoai/DEMO_SCRIPT.md` for presenting the full lifecycle live, and
`deploy/rhoai/GENERALIZABILITY.md` for why this is a reusable "unified context layer for agent
action on RHOAI" contract rather than a one-off lending example.

## Evaluations

```
python3 -m src.eval_harness   # inject real bugs + the upstream-contract scenario, score them
python3 -m src.eval_report    # bucket results (+ any real demo runs) into 4 categories
```

`eval_report.py` never merges deterministic, real-infrastructure, scripted-model, and
live-model results into one success rate — each is reported separately, and a category with
no real data behind it is reported unavailable, never fabricated.

## Tests

```
python3 -m pytest
```

`src/legacy/` holds an earlier, smaller single-scenario prototype (3 tables, one pandas
transform, three canned incidents) — superseded by the system above, kept for reference.
