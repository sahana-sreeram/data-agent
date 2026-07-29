# RHOAI deployment runbook

This is the exact, real command sequence that took this demo from "manifests authored, never
applied" to a **fully working, live-verified cluster-backed repair flow** reaching
`VERIFIED_PENDING_PR` on real RHOAI infrastructure (confirmed live, 2026-07-29). Every command
below was actually run against `https://api.prod.rhoai.rh-aiservices-bu.com:6443`, project
`data-agent`. Re-running this sequence against a fresh/recreated `data-agent` project should
reproduce the same end state.

Nothing here pushes, merges, or promotes application code automatically. `verify_candidate_repair`
reaching `VERIFIED_PENDING_PR` produces a local, unpushed git branch only -- promotion still
requires the separate, explicit human-accept action (`src.data_ops.accept_repair`, not yet
wired into the console for RHOAI as of this runbook).

## 0. Confirm authentication

```
oc whoami
oc project
```

OpenShift login tokens expire (observed: ~24h). If `oc whoami` fails with `Unauthorized`:
```
oc login --server=https://api.prod.rhoai.rh-aiservices-bu.com:6443 --web
```

## 1. Inspect the cluster (read-only, confirmed live 2026-07-28/29)

- OpenShift 4.22, Kubernetes v1.30.12.
- RHOAI/OpenDataHub is installed cluster-wide.
- Spark Operator is installed: `sparkapplications.sparkoperator.k8s.io/v1beta2` is a real CRD.
- The authenticated user has project-admin-shaped rights in `data-agent` only (not
  cluster-admin) -- can create sparkapplications/deployments/rolebindings/secrets in this one
  namespace.
- `openshift-storage.noobaa.io` (in-cluster S3-compatible object storage) exists as a
  StorageClass but is **administratively disabled** (`opendatahub.io/sc-config` annotation
  carries `isEnabled:false`) -- an ObjectBucketClaim against it sits in `Pending` forever with
  zero events. **Do not spend time debugging this** -- use the dedicated in-cluster MinIO
  path (step 4) instead.
- The namespace has a `LimitRange` (`data-agent-core-resource-limits`) that auto-injects a
  default 500m CPU limit / 256Mi memory request on any container without explicit values, and
  caps any single container at 10 CPU / 24Gi memory.

## 2. Build and push both images (on-cluster binary build, no external registry needed)

Confirmed the user has local Docker AND `buildconfig`/`imagestream` create permission in
`data-agent` -- building on-cluster avoids needing any external registry credentials.

### 2a. Console + both MCP servers (`data-agent:latest`)

```
oc new-build --binary --strategy=docker --name=data-agent -n data-agent \
  -l app.kubernetes.io/managed-by=claude-demo
oc patch bc/data-agent -n data-agent --type=json \
  -p='[{"op":"add","path":"/spec/strategy/dockerStrategy/dockerfilePath","value":"deploy/rhoai/Dockerfile"}]'
# --exclude="" is REQUIRED: oc start-build --from-dir excludes .git by default, but
# src.pr_artifact's create_pr path needs a real .git history in the image (see Dockerfile).
oc start-build data-agent -n data-agent --from-dir=. --exclude="" --follow
```

### 2b. Spark driver/executor image (`data-agent-spark:latest`) -- DIFFERENT image, DIFFERENT base

```
oc new-build --binary --strategy=docker --name=data-agent-spark -n data-agent \
  -l app.kubernetes.io/managed-by=claude-demo
oc patch bc/data-agent-spark -n data-agent --type=json \
  -p='[{"op":"add","path":"/spec/strategy/dockerStrategy/dockerfilePath","value":"deploy/rhoai/Dockerfile.spark"}]'
oc start-build data-agent-spark -n data-agent --from-dir=. --follow
```

**Why two images**: the Spark Operator's driver/executor pods rely on the image's own
`ENTRYPOINT` being a real Spark k8s bootstrap script (`/opt/entrypoint.sh`, only present in
official Apache Spark images) -- confirmed live that the operator only ever *appends* args
like `driver --properties-file ...` to whatever the image's entrypoint already is, never
overrides it, so the console's own `uvicorn` entrypoint breaks job submission outright. See
`deploy/rhoai/Dockerfile.spark`'s header comment.

## 3. Deploy RBAC

```
oc apply -f deploy/rhoai/serviceaccount.yaml
oc apply -f deploy/rhoai/role.yaml
oc apply -f deploy/rhoai/rolebinding.yaml
```

`role.yaml` grants `get/list/watch/create/delete/deletecollection` on
`sparkapplications`/`pods`/`pods/log`/`services`/`configmaps`/`persistentvolumeclaims` --
`deletecollection` is a SEPARATE verb from `delete` and is required (confirmed live via real
`Forbidden` errors during the Spark driver's own cleanup, which deletes-by-label-selector).

## 4. Deploy storage: dedicated in-cluster MinIO (NooBaa is disabled -- see step 1)

Choose your own MinIO credentials for this ephemeral demo instance and export them locally
first -- never hardcode or commit the actual values (referred to below as `$MINIO_ACCESS_KEY`/
`$MINIO_SECRET_KEY`):
```
export MINIO_ACCESS_KEY=<choose-one>
export MINIO_SECRET_KEY=<choose-one>

oc create secret generic minio-credentials -n data-agent \
  --from-literal=MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
  --from-literal=MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | oc label -f - app.kubernetes.io/managed-by=claude-demo --local -o yaml | oc apply -f -
oc apply -f deploy/rhoai/minio-deployment.yaml
oc rollout status deployment/minio -n data-agent --timeout=90s
```

Create the bucket:
```
oc exec deployment/minio -n data-agent -- sh -c '
mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb local/data-agent
'
```

Seed it with the same mock lending data local dev uses (mirrors local MinIO -> cluster MinIO
over a port-forward -- run from your own machine, with local MinIO already running via
`docker compose up`; `$LOCAL_MINIO_ACCESS_KEY`/`$LOCAL_MINIO_SECRET_KEY` are whatever your own
`.env` already has):
```
oc port-forward svc/minio -n data-agent 19000:9000 &
mc alias set localminio http://localhost:9000 "$LOCAL_MINIO_ACCESS_KEY" "$LOCAL_MINIO_SECRET_KEY"
mc alias set clusterminio http://localhost:19000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc mirror --overwrite localminio/data-agent/raw/ clusterminio/data-agent/raw/
mc mirror --overwrite localminio/data-agent/context/ clusterminio/data-agent/context/
kill %1  # stop the port-forward
```

Create the event-log directory placeholder Spark's history logging requires to already exist
(a zero-byte object at the exact directory key -- `mc pipe` can't create this due to path
parsing, use boto3 directly):
```
oc port-forward svc/minio -n data-agent 19000:9000 &
python3 -c "
import boto3, os
client = boto3.client('s3', endpoint_url='http://localhost:19000', aws_access_key_id=os.environ['MINIO_ACCESS_KEY'], aws_secret_access_key=os.environ['MINIO_SECRET_KEY'], region_name='us-east-1')
client.put_object(Bucket='data-agent', Key='spark-events/', Body=b'')
"
kill %1
```

## 5. Create the app Secret (S3 credentials only -- OpenAI key deferred, see step 9)

Reuses the same `$MINIO_ACCESS_KEY`/`$MINIO_SECRET_KEY` chosen in step 4 (the app's S3
credentials ARE the MinIO instance's credentials, for this dedicated-per-demo MinIO setup):
```
oc create secret generic data-agent-secrets -n data-agent \
  --from-literal=S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  --from-literal=S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | oc label -f - app.kubernetes.io/managed-by=claude-demo --local -o yaml | oc apply -f -
```

## 6. Deploy ConfigMaps

```
oc apply -f deploy/rhoai/configmap-app-config.yaml
oc apply -f deploy/rhoai/configmap-spark-defaults.yaml
```

`configmap-app-config.yaml` sets `HOME=/tmp` for every pod using it -- confirmed live that
OpenShift runs these pods as an arbitrary non-root UID with `HOME=/` (owned by root, not
writable), which breaks PySpark's Ivy-based `spark.jars.packages` dependency resolution
(needed by `get_spark_session()`'s local-mode Spark sessions, used by the console/MCP pods --
not by the Spark Operator's own driver/executor pods, which have their jars baked in instead).

## 7. Prove one real Spark job (manual, standalone -- optional sanity check before MCP)

```
oc apply -f deploy/rhoai/sparkapplication-loan-portfolio.yaml
oc get sparkapplications -n data-agent -w
oc get pods -n data-agent -l pipeline=loan_portfolio
oc logs <driver-pod-name> -n data-agent
```

Confirmed live: real driver + real executor pod, `COMPLETED`, real output written to
`curated/loan_portfolio.parquet` (162 loans, real computed metrics).

## 8. Deploy Spark History Server

```
oc apply -f deploy/rhoai/history-server-deployment.yaml
oc rollout status deployment/spark-history-server -n data-agent --timeout=90s
```

Reuses the `data-agent-spark` image (not a third-party image) via `spark-class` directly
(NOT `start-history-server.sh`, which daemonizes the JVM into the background and exits --
wrong for a container's main process). Verify:
```
oc port-forward svc/spark-history-server -n data-agent 18080:18080 &
curl -s http://localhost:18080/api/v1/applications | python3 -m json.tool
kill %1
```

## 9. Deploy both MCP servers

```
oc apply -f deploy/rhoai/mcp-spark-runtime-deployment.yaml
oc apply -f deploy/rhoai/mcp-data-ops-deployment.yaml
oc rollout status deployment/mcp-spark-runtime -n data-agent --timeout=90s
oc rollout status deployment/mcp-data-ops -n data-agent --timeout=90s
```

`mcp-data-ops-deployment.yaml` sets explicit resource requests/limits (2Gi/4Gi memory) --
confirmed live it gets OOMKilled under the namespace's 1536Mi default running two concurrent
Spark JVMs (the main process's session + `verify_candidate_repair`'s pytest-subprocess
session).

To test `create_candidate_repair`/`verify_candidate_repair` for free before connecting a real
model, enable the scripted-model path (loan_portfolio/`payment_service`-contract-change
scenario only -- see `src.demo.enterprise_incident`):
```
oc set env deployment/mcp-data-ops -n data-agent USE_SCRIPTED_MODEL=true
```

For a live demo of what the context layer itself buys you, `DEMO_CONTEXT_MODE=blind` (see
`src/context_retriever.py::BlindContextRetriever` and
`src/lifecycle_diagnostic_tools.py::LifecycleDiagnosticTools.blind_raw_context`) swaps out
metric definitions, lineage, structural pipeline metadata, raw ETL source, AND raw
`context/business_rules.json` -- for both the outer MCP tools AND `create_candidate_repair`'s
own inner diagnosis agent (one env var covers both, since they run in the same pod). Run the
same incident once with this unset (full context) and once with it set, both against the real
model, and compare the diagnosis:
```
oc set env deployment/mcp-data-ops -n data-agent DEMO_CONTEXT_MODE=blind
# ...call create_candidate_repair again, compare diagnosis confidence/evidence/target_file...
oc set env deployment/mcp-data-ops -n data-agent DEMO_CONTEXT_MODE-
```
Confirmed live (ROSA, 2026-07-29), in two stages:
1. The narrower version (context layer only, raw code/rules still visible) did NOT change
   gpt-5's diagnosis for the flagship incident -- it reconstructed the same `HIGH`-confidence,
   correct answer from raw ETL source + raw business rules + data-aggregation tools alone.
2. Widening blind mode to also withhold raw code/rules (the current behavior) DID produce a
   real, measurable gap on the identical incident: confidence dropped `HIGH` -> `MEDIUM`, and
   the diagnosis explicitly flagged its own uncertainty --
   *"The exact current contents of context/business_rules.json could not be retrieved (context
   layer disabled), so we inferred it still lists 'PAID' and not 'SETTLED'"* -- listing the
   exact blinded artifacts (`context/business_rules.json`, the ETL source snippet) under
   `additional_evidence_needed`. The model still reached the right general conclusion from raw
   data aggregation alone, but visibly shifted from confirmed fact to honest, flagged
   inference -- a more credible demo point than a binary right/wrong: **the context layer is
   what turns "we think, based on inference" into "we know, confirmed directly."**

Manual test via a real MCP client (from your own machine, port-forwarded):
```python
# pip install mcp, then:
import asyncio, json
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

async def main():
    async with streamable_http_client('http://localhost:18001/mcp') as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool('get_data_product_context', {'pipeline_name': 'loan_portfolio'})
            print(result.content[0].text)

asyncio.run(main())
```
(`oc port-forward svc/mcp-data-ops -n data-agent 18001:8000` / `svc/mcp-spark-runtime ... 18000:8000` first.)

## 10. Inject the semantic-failure scenario (payment_service v2, PAID -> SETTLED)

Run from your own machine, pointed at the cluster's MinIO over a port-forward -- this reuses
`src.demo.enterprise_incident.inject_contract_change` exactly, just against cluster storage
instead of local:
```
oc port-forward svc/minio -n data-agent 19000:9000 &
S3_ENDPOINT_URL=http://localhost:19000 \
S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
S3_BUCKET=data-agent \
JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
python3 -c "
from src.storage import S3Storage
from src.demo.enterprise_incident import inject_contract_change
print(inject_contract_change(S3Storage(), None))
"
kill %1
```

Confirmed live: `loan_portfolio` becomes untrusted (`total_outstanding_principal_status_vocabulary_drift`
fails) despite every job (event generation, ingestion, the real RHOAI Spark run) reporting
success -- the exact flagship narrative, now proven with the Spark job running for real on
the cluster.

## 11. Run the full cluster-backed repair flow (scripted model)

Via the deployed data-ops MCP server (`USE_SCRIPTED_MODEL=true` from step 9):
```python
result = await session.call_tool('create_candidate_repair', {
    'pipeline_name': 'loan_portfolio',
    'approve_categories': ['SOURCE_CONTRACT_CHANGE'],  # SOURCE_CONTRACT_CHANGE is refused by default policy
})
repair_id = json.loads(result.content[0].text)['repair_id']

result = await session.call_tool('verify_candidate_repair', {'repair_id': repair_id})
# -> verification_status: VERIFIED_PENDING_PR, tests PASS, real branch, real diff
```

Confirmed live end to end, 2026-07-29 -- reaches `VERIFIED_PENDING_PR` with a real local git
branch, real diff (`context/pipeline_rules/loan_portfolio.json` repointed to
`context/business_rules_settled_adopted.json`), real before/after metrics, `tests.targeted`
and `tests.full_relevant_suite` both `PASS`.

## Real bugs found and fixed getting here (each documented inline where the fix lives)

1. NooBaa storage class administratively disabled -> dedicated in-cluster MinIO
   (`configmap-app-config.yaml`, `minio-deployment.yaml`).
2. `get_spark_session()` hardcoded `.master("local[*]")`, would silently defeat real
   distributed execution under the Spark Operator -> gated on `SPARK_APPLICATION_ID` presence
   (`src/spark_session.py`).
3. Spark Operator's own controller pod has a non-writable Ivy cache -> jars baked into
   `data-agent-spark` at build time instead of `spec.deps.packages` (`Dockerfile.spark`,
   `sparkapplication-loan-portfolio.yaml`).
4. Namespace `LimitRange` defaults to 500m CPU limit -> explicit `coreLimit` on
   driver/executor.
5. Console's `uvicorn` entrypoint isn't a real Spark k8s bootstrap script -> separate
   `Dockerfile.spark` built from `apache/spark:3.5.5-python3`.
6. This Spark Operator build silently drops `spec.driver.env`/`envFrom` (a known upstream
   issue, kubeflow/spark-operator#1108) -> Spark's own native
   `spark.kubernetes.driver.secretKeyRef.*`/`driverEnv.*` config keys instead
   (`sparkapplication-loan-portfolio.yaml`, `pipeline_runner.py`).
7. Spark's event-log writer requires the S3 directory to already exist -> directory-marker
   object (step 4); missing `deletecollection` RBAC verb for the driver's own cleanup
   (`role.yaml`).
8. `read_namespaced_pod_log`'s default response deserialization is broken in this
   `kubernetes` client version (returns `str(raw_bytes)`, not decoded text) ->
   `_preload_content=False` + manual decode (`runtime_inspector.py`).
9. `pyarrow` was never actually declared as a dependency anywhere despite being required ->
   added to `pyproject.toml` core deps.
10. `pytest` was a `dev`-only extra, but `verify_candidate_repair`'s targeted-test rerun needs
    it at runtime, not just for this repo's own test suite -> moved to core deps; `tests/`
    wasn't copied into the image at all -> added `COPY tests ./tests`.
11. `run_lifecycle_self_healing`'s `human_approved_categories` override only allowed
    `mode="create_pr"`, but `create_candidate_repair` uses `mode="propose_patch"` (which
    structurally also can't promote) -> validation relaxed to allow both
    (`lifecycle_run_self_healing.py`); `create_candidate_repair` never exposed an approval
    parameter at all -> added `approve_categories` (`data_ops_server.py`).
12. `mcp-data-ops` OOMKilled under the namespace's default 1536Mi running two concurrent Spark
    JVMs -> explicit resource requests/limits.
13. Unpinned `pandas`/`pyspark` resolved to different, incompatible versions in the deployed
    image (pandas 3.0.5 + pyspark 4.2.0) vs. local dev (pandas 3.0.3 + pyspark 4.1.2) -- the
    same test passed 5/5 locally and failed 2/5 in the image with a pandas/PySpark Arrow
    interop bug (`ValueError: assignment destination is read-only`) -> pinned both exactly to
    the versions the full local test suite (775+ tests) is verified against.
14. `src.pr_artifact`'s `create_pr` path needs `git` (not installed) AND a real `.git` history
    (never copied into the image, and `oc start-build --from-dir` excludes `.git` by default)
    AND a configured git identity (a fresh container has none) -> installed `git`, added
    `COPY .git ./.git` (built with `--exclude=""` to override the default exclusion), and
    `git config --system user.name/user.email` + `safe.directory` (`--system`, not `--global`,
    since the runtime container's `HOME=/tmp` differs from the build-time root user's).
15. `HADOOP_AWS_PACKAGE` was hardcoded to `3.4.2`, mismatched against pyspark 4.2.0's bundled
    3.5.0 Hadoop client version -> made version-detection dynamic instead of hardcoded
    (globs for `hadoop-client-api-*.jar` in whatever pyspark is actually installed) --
    superseded by #13's version pin, but the dynamic detection is kept as a defense-in-depth
    correctness fix regardless of what gets installed in the future.
16. Submitting all 6 pipelines' SparkApplications back-to-back (as the real OpenAI model did on
    its first unguided run of `codex_mcp_loop` against all 6 pipelines, 2026-07-29) overwhelms
    this cluster's Spark Operator controller -- every driver pod except the first got scheduled
    but its `spark-drv-*-conf-map` ConfigMap was never created, so all 5 later drivers sat in
    `ContainerCreating` forever (confirmed live via `FailedMount ... configmap ... not found`
    events; cleaned up via `oc delete sparkapplication` + `oc delete pod --force`). Fixed by
    tightening `SYSTEM_PROMPT` (`src/agents/codex_mcp_loop.py`) to require confirming (via
    `get_spark_application_status`) that one pipeline's job has left `SUBMITTED` before
    submitting the next -- a prompt-level fix, since the Spark Operator's own concurrency
    handling isn't something this project controls.
17. Two compounding issues surfaced together on a second cluster (ROSA, 2026-07-29) once bug
    #16's fix was in place: (a) requiring one-at-a-time submission costs more model turns than
    the old (unsafe) back-to-back behavior -- a real 6-pipeline run exhausted
    `DEFAULT_MAX_TURNS=16` with every one of its OpenAI calls succeeding, purely from the extra
    status-check turns the sequential-submission requirement now adds -- fixed by raising
    `DEFAULT_MAX_TURNS` to 40 (`src/agents/codex_mcp_loop.py`). (b) That `ModelClientError`
    failure exited non-zero, and the Job's unset `backoffLimit` defaulted to Kubernetes' normal
    6 retries -- a second full run silently started behind our backs, resubmitting
    SparkApplications for pipelines the first run had already processed and burning more real,
    billed OpenAI calls with nobody watching. Fixed by setting `backoffLimit: 0` on the
    CronJob's `jobTemplate` (`morning-loop-cronjob.yaml`) -- a run that didn't converge needs a
    human to look at it, not an automatic retry storm.
18. `accept_repair` ran `git merge --no-ff <branch>` against whatever repo the CURRENT process
    happened to be in -- fine locally (one long-lived process), broken on a real cluster:
    `create_candidate_repair`/`verify_candidate_repair` create their branch inside the
    `mcp-data-ops` pod's own ephemeral git repo, but `accept_repair` only runs via
    `/api/repairs/accept` on the **console** pod -- a different pod with its own independent,
    ephemeral git history that never saw that branch (confirmed live, ROSA, 2026-07-29:
    `git merge failed: merge: repair/2226af983ee3 - not something we can merge`). Even calling
    it from the *same* pod wouldn't help long-term -- any redeploy/restart wipes the branch
    too. Fixed by falling back to `git apply`-ing the pending-repair record's own stored
    unified diff directly when the branch merge fails (`src/data_ops.py::accept_repair`) --
    the diff is already persisted as DATA in the state store, not live git state, so applying
    it works regardless of which pod/process created the candidate and survives restarts.
    Confirmed live: `accept_repair` on ROSA correctly detected the unmergeable branch, applied
    the stored diff instead, reran the real pipeline, and `loan_portfolio` came back
    `validation_status=PASS` on real cluster data.

## Real-world friction encountered (not project bugs, but worth knowing about)

- **Docker Hub anonymous pull rate limit**: `oc start-build` pulls `python:3.12-slim` from
  `docker.io` unauthenticated (100 pulls/6h per source IP) -- on both RHOAI and ROSA this was
  hit repeatedly during a day of iterating on builds, especially on a cloud-hosted cluster
  sharing a NAT gateway IP with other tenants. Retrying every few minutes always eventually
  got through (once within 1 retry, once after ~20 min). If this becomes a recurring blocker,
  authenticate the `builder` ServiceAccount's pulls: create a Docker Hub access token, then
  `oc create secret docker-registry dockerhub-pull-secret --docker-server=docker.io
  --docker-username=<user> --docker-password=<token>` + `oc secrets link builder
  dockerhub-pull-secret --for=pull` -- authenticated pulls get a materially higher limit.
- **OpenAI `insufficient_quota`**: this is an org/account-level billing gate, not a per-key or
  per-model limit -- a new key under the same org hits the identical error immediately, and
  switching to a cheaper model doesn't help either, since the gate applies before OpenAI even
  looks at which model was requested. Only fixed by raising the account's spending limit/adding
  funds at `platform.openai.com`.
- **`/api/repairs/accept`'s real Spark rerun can exceed a Route's default gateway timeout**
  (observed: a 504 after ~30s on ROSA) even though the request completes successfully
  server-side seconds later -- check `oc logs deployment/data-agent-console` and
  `GET /api/repairs/pending` (should be empty) / `GET /api/run-details/latest` to confirm the
  real outcome rather than trusting the HTTP response alone if this endpoint is ever
  hit directly instead of through a client with a longer timeout.

## What is NOT done yet

- **Other 5 pipelines' SparkApplication YAML manifests** (`sparkapplication-*.yaml`, one per
  pipeline) -- templated and present in this repo but not individually `oc apply`'d/validated
  standalone; the morning loop's own `submit_spark_pipeline` tool builds its own
  `SparkApplication` object dynamically instead of applying these files, and that dynamic path
  IS proven for all 6 pipelines (see bug #16/#17's live runs). These static per-pipeline
  manifests remain useful for a manual, single-pipeline sanity check (as step 7 does for
  `loan_portfolio`) but aren't required for the full demo to work.

Everything else in the original plan -- real Codex/OpenAI, console + Route, CronJob (including
a full real, all-6-pipeline run), and `accept_repair` promotion -- has now been proven live on
two separate clusters (RHOAI, 2026-07-29; ROSA, 2026-07-29).

## Reproducing from scratch (e.g. after deleting and recreating the `data-agent` project)

Run steps 2-11 in order. Everything is either a committed file in this repo (`deploy/rhoai/*`,
`Dockerfile*`) or an exact command above -- nothing else was done by hand. Steps 2 (image
builds) are the slowest (~2-5 min each); everything else is fast once the images exist.

On a cluster that has never run this before, also confirm the Spark Operator itself is
installed (`oc get crd sparkapplications.sparkoperator.k8s.io`) -- it is NOT an OperatorHub
package for the `sparkoperator.k8s.io/v1beta2` API this project targets; install via Helm:
```
helm repo add spark-operator https://kubeflow.github.io/spark-operator && helm repo update
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace \
  --set webhook.enable=true \
  --set controller.podSecurityContext.fsGroup=null --set webhook.podSecurityContext.fsGroup=null \
  --set-json 'spark.jobNamespaces=["data-agent"]'
```
The two `--set ...podSecurityContext.fsGroup=null` overrides are required on OpenShift/ROSA:
the chart hardcodes `fsGroup: 185`, which every real SCC on OpenShift rejects (confirmed live,
ROSA, 2026-07-29) -- setting it to null lets OpenShift assign one from the namespace's allowed
range instead. `spark.jobNamespaces` (not the top-level `jobNamespaces` value -- confirmed live
that setting the wrong one silently renders to `--namespaces=default` and the operator never
sees any SparkApplication created in a different namespace) must include your project's name,
or the operator's controller never sees any `SparkApplication` created there at all -- it just
sits with no `status` and no events, forever.
