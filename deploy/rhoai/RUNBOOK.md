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

## What is NOT done yet

- **Real Codex/OpenAI**: `USE_SCRIPTED_MODEL=true` proves the infrastructure; connecting a
  real model means creating a Secret with a real `OPENAI_API_KEY` from your local environment
  (never printed):
  ```
  oc create secret generic data-agent-secrets -n data-agent --dry-run=client -o yaml \
    --from-literal=S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    --from-literal=S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    --from-literal=OPENAI_API_KEY="$OPENAI_API_KEY" | oc apply -f -
  oc set env deployment/mcp-data-ops -n data-agent USE_SCRIPTED_MODEL-
  ```
  then rerun step 11 and evaluate whether the real model follows the intended tool sequence,
  stops when evidence is insufficient, and converges without looping.
- **Console deployment** (`deploy/rhoai/console-deployment.yaml`) and the **Route**
  (`route-run-details.yaml`) -- not yet applied. Same image/fixes as the MCP servers apply
  (git, resource limits may be needed too since the console can also trigger repairs).
- **CronJob** (`morning-loop-cronjob.yaml`) -- not yet applied; calls
  `src.agents.codex_mcp_loop`, which has not yet been run against the live cluster at all
  (only tested against fakes locally).
- **Other 5 pipelines' SparkApplications** -- only `loan_portfolio` (the flagship scenario)
  has been proven on RHOAI. The others would need their own SparkApplication manifests
  (templated from `sparkapplication-loan-portfolio.yaml`) if the full morning-loop demo
  should run against RHOAI for every pipeline, not just the flagship one.
- **`accept_repair`/promotion on RHOAI** -- `verify_candidate_repair` reaching
  `VERIFIED_PENDING_PR` is the end of what's been proven; a human explicitly accepting that
  candidate (a real git merge + rerun) has only ever been tested against the local path.

## Reproducing from scratch (e.g. after deleting and recreating the `data-agent` project)

Run steps 2-11 in order. Everything is either a committed file in this repo (`deploy/rhoai/*`,
`Dockerfile*`) or an exact command above -- nothing else was done by hand. Steps 2 (image
builds) are the slowest (~2-5 min each); everything else is fast once the images exist.
