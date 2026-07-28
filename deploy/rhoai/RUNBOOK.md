# RHOAI deployment runbook

This is the exact command sequence for taking this demo from "manifests authored, never
applied" to "running on the real cluster." Steps 1-3 have already been run once (results noted
inline, dated) as part of building these manifests; rerun them yourself to confirm current
state before proceeding, since cluster state can drift. Steps 4 onward have **not** been run
against the live cluster by anything in this repo — no image has been built or pushed, no
Secret created, nothing deployed. Nothing here pushes, merges, or promotes application code
automatically; that boundary is unrelated to and unaffected by this deployment process.

## 1. Confirm you're authenticated

```
oc whoami
oc project
```

Confirmed live (2026-07-28): `ssreeram@redhat.com`, project `data-agent`, cluster
`https://api.prod.rhoai.rh-aiservices-bu.com:6443`. OpenShift login tokens expire (observed:
~24h) — if `oc whoami` fails with `Unauthorized`, re-run `oc login --web` (or paste a fresh
token from the OpenShift console's "Copy login command" page) before continuing.

## 2. Inspect the cluster — do not assume cluster-admin access

```
oc version
oc get csv -A | grep -iE "rhods|opendatahub"
oc api-resources | grep -iE "spark|datascience|opendatahub"
oc get projects
oc auth can-i --list -n data-agent
oc get routes -n data-agent
oc get sc
oc get all -n data-agent
```

Confirmed live (2026-07-28):
- Cluster: OpenShift 4.22, Kubernetes v1.30.12.
- RHOAI/OpenDataHub **is installed** cluster-wide (`*.opendatahub.io`/`*.platform.opendatahub.io`
  API groups present: dashboards, notebooks, datasciencepipelines, workbenches, kserve, etc.).
- Spark Operator **is installed**: `sparkapplications.sparkoperator.k8s.io/v1beta2` and
  `scheduledsparkapplications` are real, registered CRDs — matches every manifest in this
  directory exactly.
- The authenticated user can only see 2 projects (`data-agent`,
  `openshift-virtualization-os-images`) and cannot list cluster-scoped `DataScienceCluster`/
  `DSCInitialization` or CRDs directly — **not cluster-admin**, consistent with a single-project
  role. Within `data-agent`, the user has broad rights (`create`/`update`/`delete`/`get`/`list`/
  `watch` on most namespaced resources including `sparkapplications`, `deployments`,
  `rolebindings`) — a project-admin-shaped role scoped to this one namespace.
- No Routes exist yet in `data-agent`. One unrelated pod (`workspace...`, a devworkspace/IDE
  session — not part of this app) is running there; left untouched.
- Storage: `ocs-storagecluster-ceph-rbd` (default, block), `ocs-storagecluster-cephfs` (file),
  and `openshift-storage.noobaa.io` (S3-compatible object storage via `ObjectBucketClaim`) are
  all available — the latter is a real in-cluster alternative to the external MinIO used for
  local dev, not yet chosen either way (see step 5).

## 3. Validate every manifest against the real cluster (non-mutating)

```
for f in deploy/rhoai/*.yaml; do
  [ "$f" = "deploy/rhoai/secret.example.yaml" ] && continue  # placeholder values only, never apply
  [ "$f" = "deploy/rhoai/namespace.yaml" ] && continue        # see that file's header comment
  echo "=== $f ==="
  oc apply --dry-run=server -f "$f"
done
```

Confirmed live (2026-07-28): every manifest except `namespace.yaml` (documented, expected
failure against the already-existing project — see its header comment) and `rolebinding.yaml`
in isolation (needs `role.yaml` applied first — expected sequencing, not a bug) validated
successfully server-side: `serviceaccount`, `role`, both `configmap`s, the `sparkapplication`,
every `deployment`+`service`, the `cronjob`, and the `route`.

## 4. Build and push the image — not yet done

Choose a registry you can push to (quay.io, the cluster's internal registry, etc.), then:

```
docker build -f deploy/rhoai/Dockerfile -t <registry>/<org>/data-agent:latest .
docker push <registry>/<org>/data-agent:latest
```

Replace every `CHANGE_ME_IMAGE_REF` in this directory's manifests with the pushed reference
(`sparkapplication-loan-portfolio.yaml`, `mcp-data-ops-deployment.yaml`,
`mcp-spark-runtime-deployment.yaml`, `console-deployment.yaml`, `morning-loop-cronjob.yaml`).
Only build/push this one image — nothing else in this demo needs a custom image (History
Server and Redis use their own official images, already referenced as-is).

## 5. Choose and confirm S3-compatible storage — not yet done

Either point at the same external MinIO used for local dev (if reachable from the cluster), or
provision an in-cluster bucket via NooBaa:

```
# Option B: in-cluster NooBaa object storage (confirmed available in step 2)
cat <<'EOF' | oc apply -f -
apiVersion: objectbucket.io/v1alpha1
kind: ObjectBucketClaim
metadata:
  name: data-agent-bucket
  namespace: data-agent
spec:
  generateBucketName: data-agent
  storageClassName: openshift-storage.noobaa.io
EOF
oc get secret data-agent-bucket -n data-agent -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d  # do not paste this output anywhere durable
```

Update `configmap-app-config.yaml`'s `S3_ENDPOINT_URL`/`S3_BUCKET` to match whichever option you
chose, before creating the Secret in step 6.

## 6. Create the real Secret from local environment variables — not yet done

**Never** `oc apply -f` a filled-in copy of `secret.example.yaml`, and never print these values:

```
oc create secret generic data-agent-secrets -n data-agent \
  --from-literal=S3_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID" \
  --from-literal=S3_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY" \
  --from-literal=OPENAI_API_KEY="$OPENAI_API_KEY"
```

## 7. Deploy — not yet done

Apply in this order (RBAC before workloads; role before rolebinding):

```
oc apply -f deploy/rhoai/serviceaccount.yaml
oc apply -f deploy/rhoai/role.yaml
oc apply -f deploy/rhoai/rolebinding.yaml
oc apply -f deploy/rhoai/configmap-app-config.yaml
oc apply -f deploy/rhoai/configmap-spark-defaults.yaml
# (secret already created in step 6)
oc apply -f deploy/rhoai/history-server-deployment.yaml
oc apply -f deploy/rhoai/mcp-data-ops-deployment.yaml
oc apply -f deploy/rhoai/mcp-spark-runtime-deployment.yaml
oc apply -f deploy/rhoai/console-deployment.yaml
oc apply -f deploy/rhoai/route-run-details.yaml
# optional:
oc apply -f deploy/rhoai/redis-deployment.yaml       # only if STATE_BACKEND=redis
oc apply -f deploy/rhoai/morning-loop-cronjob.yaml    # leave suspend:true until step 10 passes once
```

## 8. Verify a real Spark run + S3 read/write — not yet done

```
oc apply -f deploy/rhoai/sparkapplication-loan-portfolio.yaml
oc get sparkapplication loan-portfolio-manual -n data-agent -w
oc get pods -n data-agent -l pipeline=loan_portfolio
```

Confirm `curated/loan_portfolio.parquet` was written to the chosen bucket (step 5) once the
SparkApplication reports `COMPLETED`.

## 9. Verify event logs reach Spark History Server — not yet done

```
oc port-forward svc/spark-history-server -n data-agent 18080:18080
```

Open http://localhost:18080 and confirm the run from step 8 appears with real stage/task
detail — this is what `SparkHistoryRuntimeInspector` (`RUNTIME_BACKEND=spark_history`) reads.

## 10. Connect Codex to the deployed MCP servers and run both loop scenarios — not yet done

```
oc port-forward svc/mcp-data-ops -n data-agent 8001:8000
oc port-forward svc/mcp-spark-runtime -n data-agent 8002:8000
```

Point an MCP-capable Codex client at `http://localhost:8001/mcp` and `http://localhost:8002/mcp`
(streamable-HTTP), or run `python3 -m src.agents.codex_mcp_loop` from inside the cluster/VPN with
`MCP_DATA_OPS_URL`/`MCP_SPARK_RUNTIME_URL` pointed at the in-cluster service DNS names instead of
the in-process servers it uses locally. Run once against the healthy baseline, then once after
introducing the `payment_service` v2 contract change (mirroring
`src.demo.enterprise_incident --inject-contract-change`) to confirm Codex reaches
`create_candidate_repair` → `verify_candidate_repair` → `VERIFIED_PENDING_PR`, visible in the
console's Run Details tab.

## 11. Document what you actually ran

Once steps 4-10 are complete against a real cluster, record the exact commands and any manifest
values you had to change (image ref, S3 endpoint/bucket, route host) back into this file so the
next person's run is a copy-paste, not a rediscovery.
