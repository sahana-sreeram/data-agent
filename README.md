# data-agent

An agentic data-operations control layer for a synthetic lending company's data estate — not
a chatbot over a table. Upstream services emit domain events; PySpark pipelines curate them
into business metrics; a context-enrichment layer derives most technical documentation
automatically, leaving only real business-semantics decisions to a human; and the system walks
a full incident lifecycle — trust check → diagnosis → governed repair → sandboxed verification
→ reviewable PR — whenever a data product can't be trusted. Q&A is one entry point into that
lifecycle, not the whole of it: the context layer, not the agent loop, is the differentiator.

## How it works

- **Ingest** — 6 upstream services (`demo/services/`) emit versioned domain events;
  `src/events_to_lifecycle_tables.py` projects them into raw tables at any scale.
- **Transform** — 6 PySpark pipelines produce curated metrics, onboarded via manifest.
- **Validate** — an independent validator reconciles each metric against raw data; a separate
  raw-contract validator catches upstream changes reconciliation alone can't.
- **Enrich** — `src/context_enrichment/` derives schemas/lineage/business-rule references from
  code and runtime; `context/human/*.yaml` holds only what a human must decide.
  `src/context_store/` merges both, surfacing mismatches as evidence rather than picking a winner.
- **Operate** — `src/data_ops.py` and the console UI narrate the full trust → diagnose → repair
  → verify → PR lifecycle. `src/ask_lifecycle.py` answers one question directly when that's all
  that's needed.
- **Self-heal** — the agent diagnoses a failure, proposes a repair, verifies it in an isolated
  sandbox, and either promotes it or opens a reviewable PR artifact — never both. Sensitive
  root-cause categories stay refused unless a human explicitly unlocks one incident at a time.
- **Onboard** — new pipelines register via manifest (`pipelines/*.yaml`), not core-code changes.
- **Evaluate** — `src/eval_harness.py`/`src/eval_report.py` score real injected bugs across four
  categories that are never merged into one number.

## Quickstart

Prerequisites: Docker, a JDK for PySpark (e.g. `brew install openjdk@17`), an OpenAI API key.

```
cp .env.example .env   # fill in OPENAI_API_KEY and JAVA_HOME
./scripts/bootstrap.sh # starts MinIO, installs deps, generates + migrates + runs ETL
python3 -m src.api     # http://127.0.0.1:8000 -- the console
```

`bootstrap.sh` is idempotent. For the event-sourced/scale path, see `demo/services/README.md`.

## Flagship demo

`payment_service` ships a contract change that every job "succeeds" against, but silently
breaks a business metric. The agent investigates, refuses to auto-repair an upstream contract
change, and only proceeds once a human approves a narrowly-scoped config fix — never the shared
business rules or the ETL source. See [`demo/README.md`](demo/README.md) to run it and
[`demo/DEMO_SCRIPT.md`](demo/DEMO_SCRIPT.md) for the full walkthrough.

## At-scale verification

Verified at 20,000 customers / 8.65M raw rows, all pipelines and validators passing.
`python3 -m src.data_ops scale` reports the current estate's measured counts.

```
python3 -m src.generate_upstream_events --profile demo --output s3 --seed 42
python3 -m src.events_to_lifecycle_tables --from s3
python3 -m src.run_lifecycle_etl_pipelines
```

## RHOAI platform mode (optional)

Everything above runs entirely locally by default. `src/platform_backends/` makes the same
lifecycle runnable on Red Hat OpenShift AI instead: Spark via the Spark Operator, runtime
evidence from Spark History Server, Codex driving the loop through MCP tool calls. See
[`deploy/rhoai/RUNBOOK.md`](deploy/rhoai/RUNBOOK.md) for deployment and env-var/MCP reference,
and [`GENERALIZABILITY.md`](deploy/rhoai/GENERALIZABILITY.md) for why this is a reusable
contract rather than a one-off lending example.

## Evaluations

```
python3 -m src.eval_harness   # inject real bugs + the upstream-contract scenario, score them
python3 -m src.eval_report    # bucket results into 4 never-merged categories
```

## Tests

```
python3 -m pytest
```
