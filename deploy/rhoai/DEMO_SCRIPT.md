# Live demo script

A concrete, orderable runbook for presenting this system live against a deployed cluster
(RHOAI or ROSA -- both proven identical this session; commands below assume the `data-agent`
namespace and the `loan_portfolio` pipeline throughout, matching every other doc in
`deploy/rhoai/`). Each act states exactly what to click/type and what it's meant to prove.
Prerequisite: everything in `RUNBOOK.md` steps 1-11 already applied, plus
`history-server-route.yaml` (Act 4) and `HISTORY_SERVER_URL` set on the console.

Get the console URL once at the start:
```
CONSOLE=https://$(oc get route data-agent-console -n data-agent -o jsonpath='{.spec.host}')
```

## Act 1 -- Ask questions in scope of the pipelines

Nothing new to build -- the Q&A tab (or `POST $CONSOLE/api/incident`) already answers against
live cluster data, citing real metrics with their source. Good example questions:
- "What's our total outstanding principal for loan_portfolio?"
- "How many loans are active vs. defaulted?"
- "What's the collection rate for payment_performance?"

Point: every answer traces through real lineage/metric context (`ContextRetriever`), not a
canned lookup -- it's citing the exact metric definition and reconciling against curated data
live.

## Act 2 -- Show Spark/infra actually being deployed

Trigger a real pipeline run (via the console, Q&A, or a direct `submit_spark_pipeline` MCP
call -- see RUNBOOK.md step 9 for the client snippet), and split-screen:
```
oc get sparkapplications -n data-agent -w
oc get pods -n data-agent -w
```
Point: this is a real driver + executor pod pair scheduled by the Spark Operator on real
Kubernetes, not a local script.

Then click "View in Spark History Server" from the new Infrastructure card on the Run Details
tab (or go straight to the History Server Route -- see Act 4) to show the real job DAG/stages
in a browser.

## Act 3 -- Introduce a bug; show auto-heal, then correct escalation

Both narrative beats reuse the one proven, fast, purely-data-driven flagship scenario
(`payment_service` renaming `PAID`->`SETTLED`) -- no image rebuild needed, ~10s to inject.

**3a. Inject it** (`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` must already be exported -- see
RUNBOOK.md step 4/5):
```
./scripts/demo/inject-bug.sh
```
Reset afterward with `./scripts/demo/reset-bug.sh` (restores raw data, reruns clean, clears
any pending repair -- idempotent, safe to run even if nothing is injected).

Point: every job (event generation, ingestion, the real Spark run) reports SUCCESS, yet
`loan_portfolio` is now silently wrong -- ask Act 1's outstanding-principal question again to
show the console/Q&A now flags it as untrusted.

**3b. Guaranteed auto-heal, via the real Codex/MCP harness:**
```
oc set env deployment/mcp-data-ops -n data-agent USE_SCRIPTED_MODEL=true
```
This does NOT make the run fake -- it only determines what `create_candidate_repair`'s own
internal diagnosis concludes (deterministic, always the already-approved settled-adopted fix)
so the run reliably reaches `VERIFIED_PENDING_PR` instead of `BLOCKED` (see 3c). The outer
Codex loop deciding *when* to call that tool still uses a real OpenAI model, and every MCP
call, Spark rerun, git worktree, and test run is identical either way.

Click **"Run Codex/MCP (loan_portfolio)"** on the Run Details tab's Workflow card -- this
launches the actual harness (`src.agents.codex_mcp_loop`) as a real Kubernetes Job (`POST
/api/codex-run/trigger`), a genuine MCP client making real network calls to
`mcp-data-ops`/`mcp-spark-runtime`, not the console's own direct-call auto-scan. Takes a
few minutes; watch it via `oc get pods -n data-agent -w` + `oc logs -f <pod>`, or refresh Run
Details once it completes. Ends at `VERIFIED_PENDING_PR` with a real diff, real before/after
metrics, real passing tests. Click **Accept** in the console's Review package card -> real
`git apply` + real pipeline rerun -> `loan_portfolio` comes back `PASS` on real cluster data.

**3c. Correct escalation (real model):** re-inject (3a again -- idempotent) and rerun with the
real model:
```
oc create secret generic data-agent-secrets -n data-agent --dry-run=client -o yaml \
  --from-literal=S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" --from-literal=S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  --from-literal=OPENAI_API_KEY="$OPENAI_API_KEY" | oc apply -f -
oc set env deployment/mcp-data-ops -n data-agent USE_SCRIPTED_MODEL-
```
Call `create_candidate_repair` again -> correctly `BLOCKED`/`HUMAN_REVIEW_REQUIRED` (the
recommended fix touches `context/business_rules.json`, a shared, cross-pipeline file outside
the auto-repair allowlist). Narrate this as the second half of the story: **the agent knows
the difference between a safe, bounded fix it can make itself and a shared-definition change
that needs a human** -- not recklessness, judgment.

## Act 4 -- Observability

Apply the Route once (if not already):
```
oc apply -f deploy/rhoai/history-server-route.yaml
oc set env deployment/data-agent-console -n data-agent \
  HISTORY_SERVER_URL=https://$(oc get route spark-history-server -n data-agent -o jsonpath='{.spec.host}')
```
Then, on the Run Details tab, the new **Infrastructure** card shows: a live link into Spark
History Server, and the latest real Spark/pod runtime evidence (`get_spark_application_status`
/`get_pod_status`/`get_spark_run_summary`) pulled straight from the same MCP tool-call data the
Workflow card already renders -- no separate system to trust.

## Act 5 -- Prove this needs the context layer

Rerun 3c's exact incident (still injected), once more with the real model, but with the
context layer disabled:
```
oc set env deployment/mcp-data-ops -n data-agent DEMO_CONTEXT_MODE=blind
```
Call `create_candidate_repair` again and compare the diagnosis side by side with 3c's. Metric
definitions, lineage, structural pipeline metadata, raw ETL source, and raw
`context/business_rules.json` are all disabled in this mode (see
`src/context_retriever.py::BlindContextRetriever` and
`LifecycleDiagnosticTools.blind_raw_context`). Confirmed live (2026-07-29): confidence drops
`HIGH` -> `MEDIUM`, and the diagnosis explicitly flags its own uncertainty --
*"the exact current contents of context/business_rules.json could not be retrieved (context
layer disabled), so we inferred..."* -- naming the very artifacts that are blinded under
`additional_evidence_needed`. The model still reaches the right general conclusion from raw
data aggregation alone, but visibly shifts from confirmed fact to flagged inference. Narrate
this precisely: **the context layer is what turns "we think, based on inference" into "we
know, confirmed directly"** -- not "the agent knows nothing without it."

Restore afterward:
```
oc set env deployment/mcp-data-ops -n data-agent DEMO_CONTEXT_MODE-
```

## Act 6 -- Generalizability pitch

See `GENERALIZABILITY.md` (linked from the main `README.md`) for the framing: this isn't a
one-off lending demo, it's built on two swappable contracts already proven this session across
two different clusters (RHOAI and ROSA) with the same code --
`src/platform_backends/`'s `PipelineRunner`/`RuntimeInspector`/`StateStore` Protocols, and the
MCP data-ops tool schema itself as the reusable "unified context layer" contract for any
RHOAI-hosted data domain.
