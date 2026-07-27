# data-agent

A generalizable, agent-ready data framework for a synthetic lending company. 6 independently
versioned upstream services emit domain events into S3-compatible object storage; 5 PySpark
ETL pipelines curate them into business metrics; a context-enrichment layer automatically
derives most of the technical documentation (schemas, joins, lineage) from the code and
runtime itself, leaving only genuine business-semantics decisions to a human; and an agent
answers business questions grounded in that context, self-healing (diagnose → repair → verify
→ promote, or propose a reviewable PR) when the data behind a question fails validation. The
context layer — not the agent loop — is the differentiator: the self-healing behavior is the
proof that the enriched context is actually useful, not the product itself.

## How it works

- **Ingest** — 6 upstream services (`services/`) each own a slice of the lending lifecycle
  (marketing, applications, underwriting, loans, payments, risk) and emit versioned domain
  events; `src/events_to_lifecycle_tables.py` projects them into curated-pipeline-ready raw
  tables. `src/generate_upstream_events.py` scales this to hundreds of thousands of customers
  in bounded-memory batches.
- **Transform** — 5 PySpark pipelines produce curated metrics: loan portfolio, campaign
  funnel, underwriting performance, payment performance, delinquency/default.
- **Validate** — an independent pandas validator recomputes each metric from raw data and
  reconciles it against the ETL output; a separate 12-table raw validator catches upstream
  contract changes reconciliation structurally cannot (see `src/eval_scenarios.py`'s
  `UpstreamContractScenario`).
- **Enrich** — `src/context_enrichment/` derives dataset schemas, ETL joins/filters/business-
  rule references, and lineage automatically from code and runtime state; a minimal
  `context/human/*.yaml` layer holds only what a human must decide (canonical metric
  definitions, approval, repair policy). `src/context_store/` merges both with an explicit
  precedence order, surfacing any mismatch as evidence rather than picking a winner.
- **Answer** — `ask_lifecycle.py` (or the FastAPI + web UI in `api.py`/`static/`) answers
  questions from curated data, grounded against the real tool results it cites.
- **Self-heal** — if an answer depends on data that failed validation, the agent diagnoses
  why, has an LLM propose a repair, verifies it against an isolated sandbox rerun
  (`src/sandbox/`), and either promotes it directly or produces a local, reviewable PR
  artifact (`src/pr_artifact.py`) instead — never both.
- **Onboard** — new pipelines/services register via a manifest (`pipelines/*.yaml`,
  `src/manifest_loader.py`) rather than core-code changes.
- **Evaluate** — `eval_harness.py` injects real ETL bugs and a genuine upstream contract
  change, and measures diagnosis/repair success, tool-call efficiency, refusal accuracy,
  context-extraction precision/recall, and latency.

## Quickstart

Prerequisites: Docker, a JDK for PySpark (e.g. `brew install openjdk@17`), and an
OpenAI API key.

```
cp .env.example .env   # fill in OPENAI_API_KEY and JAVA_HOME
./scripts/bootstrap.sh # starts MinIO, installs deps, generates + migrates + runs ETL
python3 -m src.api     # http://127.0.0.1:8000
```

`bootstrap.sh` is idempotent — safe to rerun any time (e.g. after regenerating data). For the
event-sourced/scale path instead of one-shot generation, see `services/README.md`.

## Tests

```
python3 -m pytest
```

`src/legacy/` holds an earlier, smaller single-scenario prototype (3 tables, one pandas
transform, three canned incidents) — superseded by the system above, kept for reference.
