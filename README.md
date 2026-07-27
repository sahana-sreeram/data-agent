# data-agent

A self-healing enterprise data agent for a synthetic lending company. It generates a 12-table
lending lifecycle, runs 5 PySpark ETL pipelines over it in S3-compatible storage, independently
validates every curated output, and answers business questions about it in natural language.
When the data behind a question fails validation, it diagnoses the cause, has an LLM propose
and apply a repair, reruns and re-validates the fix in isolation, and promotes it only if
everything passes — otherwise it refuses to answer rather than guess.

## How it works

- **Generate & load** — `generate_data.py` builds the synthetic 12-table dataset;
  `migrate_lifecycle_to_s3.py` loads it into S3-compatible storage (MinIO).
- **Transform** — 5 PySpark pipelines produce curated metrics: loan portfolio, campaign funnel,
  underwriting performance, payment performance, delinquency/default.
- **Validate** — an independent pandas validator recomputes each metric from raw data and
  reconciles it against the ETL output.
- **Answer** — `ask_lifecycle.py` (or the FastAPI + web UI in `api.py`/`static/`) answers
  questions from curated data, grounded against the real tool results it cites.
- **Self-heal** — if an answer depends on data that failed validation, the agent diagnoses why,
  has an LLM propose a repair, verifies it against an isolated rerun of the ETL and validator,
  and promotes it only on a full pass.
- **Evaluate** — `eval_harness.py` injects real ETL bugs and measures diagnosis/repair success,
  tool-call efficiency, refusal accuracy, and latency.

## Quickstart

```
cp .env.example .env   # OPENAI_API_KEY, S3/MinIO creds, JAVA_HOME
python3 -m src.generate_data --output-dir data/lifecycle/raw
python3 -m src.migrate_lifecycle_to_s3
python3 -m src.run_lifecycle_etl_pipelines
python3 -m src.api   # http://127.0.0.1:8000
```

## Tests

```
python3 -m pytest
```

`src/legacy/` holds an earlier, smaller single-scenario prototype (3 tables, one pandas
transform, three canned incidents) — superseded by the system above, kept for reference.
