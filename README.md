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

- **Ingest** — 6 upstream services (`demo/services/`) each own a slice of the lending
  lifecycle (marketing, applications, underwriting, loans, payments, risk) and emit versioned
  domain events; `src/events_to_lifecycle_tables.py` projects them into curated-pipeline-ready
  raw tables. `src/generate_upstream_events.py` scales this to hundreds of thousands of
  customers in bounded-memory batches — see "At-scale verification" below.
- **Transform** — 6 PySpark pipelines produce curated metrics: loan portfolio, campaign
  funnel, underwriting performance, payment performance, delinquency/default, coupon
  performance (onboarded purely via manifest, see "Onboard" below).
- **Validate** — an independent pandas validator recomputes each metric from raw data and
  reconciles it against the ETL output; a separate 12-table raw validator catches upstream
  contract changes reconciliation structurally cannot (see the flagship demo).
- **Enrich** — `src/context_enrichment/` derives dataset schemas, ETL joins/filters/business-
  rule references, and lineage (including which upstream service produces each raw table) from
  code and runtime state; a minimal `context/human/*.yaml` layer holds only what a human must
  decide (canonical metric definitions, approval, repair policy). `src/context_store/` merges
  both with an explicit precedence order, surfacing any mismatch as evidence rather than
  picking a winner.
- **Operate** — `src/data_ops.py` and the console UI (`static/`) walk the full business-signal
  → trust-check → incident → diagnosis → repair → verify → PR-artifact lifecycle, narrated.
  `src/ask_lifecycle.py` (`/api/ask`) answers a single question directly when that's all that's
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
- **Evaluate** — `src/eval_harness.py` injects real ETL bugs and a genuine upstream contract
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
event-sourced/scale path instead of one-shot generation, see `demo/services/README.md`.

## Flagship demo: an upstream contract change, not a code bug

`payment_service` ships a v2 contract renaming a successfully-collected installment's status
from `PAID` to `SETTLED`. Event generation, ingestion, and the Spark ETL all report success —
the data product is still silently wrong, caught only by independent validation. The agent
investigates, correctly refuses to auto-repair an upstream contract change, and only proceeds
once a human explicitly approves a candidate for that one incident — repointing a narrowly
scoped, pipeline-owned config file, never the shared business rules or the ETL source.

See [`demo/README.md`](demo/README.md) to run it (locally or against a live RHOAI/ROSA
cluster) and [`demo/DEMO_SCRIPT.md`](demo/DEMO_SCRIPT.md) for the full live walkthrough.

## At-scale verification

Verified for real at 20,000 customers (not simulated or projected): 8.65M raw rows across 12
tables, all 6 ETL pipelines and their validators passing at scale. `python3 -m src.data_ops
scale` reports the current estate's measured (never hardcoded) customer/row/file/byte counts.

```
python3 -m src.generate_upstream_events --profile demo --output s3 --seed 42
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```

Known limitation: the flagship demo's injection scenario reconstructs a fixed 300-customer
payment subset regardless of current scale, so running it against a 20K-customer estate
produces mismatched loan_ids and silently defeats the reconciliation check. The live demo
environment is kept at the original small scale so the scenario stays correct; the 20K proof
above is a one-time, documented verification, not the permanent environment state.

## RHOAI platform mode (optional)

Everything above runs entirely locally by default. `src/platform_backends/` additionally makes
the same lifecycle runnable as a real platform capability on Red Hat OpenShift AI: Spark
submitted via the Spark Operator, runtime evidence pulled from Spark History Server/OpenShift,
and Codex driving the whole loop through MCP tool calls instead of direct Python calls.

`deploy/rhoai/` has the Kubernetes/OpenShift manifests and [`RUNBOOK.md`](deploy/rhoai/RUNBOOK.md)
for deploying them (including the backend-selection env vars and MCP server tool reference).
See [`GENERALIZABILITY.md`](deploy/rhoai/GENERALIZABILITY.md) for why this is a reusable
"unified context layer for agent action on RHOAI" contract rather than a one-off lending
example.

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
