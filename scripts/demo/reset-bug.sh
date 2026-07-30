#!/usr/bin/env bash
# Reverts scripts/demo/inject-bug.sh's incident against the cluster's MinIO: restores
# raw/{payment_schedule,payment_events} to pre-injection bytes, reruns loan_portfolio clean,
# and clears any pending repair record. Idempotent: safe to run even if nothing is injected.
#
# Requires: `oc` logged into the target cluster, and MINIO_ACCESS_KEY/MINIO_SECRET_KEY already
# exported in your shell (never hardcode these -- see deploy/rhoai/RUNBOOK.md step 4/5).
set -euo pipefail

: "${MINIO_ACCESS_KEY:?set MINIO_ACCESS_KEY first (see deploy/rhoai/RUNBOOK.md)}"
: "${MINIO_SECRET_KEY:?set MINIO_SECRET_KEY first (see deploy/rhoai/RUNBOOK.md)}"

NAMESPACE="${DATA_AGENT_NAMESPACE:-data-agent}"

oc port-forward "svc/minio" -n "$NAMESPACE" 19000:9000 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null' EXIT
sleep 3

S3_ENDPOINT_URL=http://localhost:19000 \
S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
S3_BUCKET=data-agent \
python3 -m src.demo.enterprise_incident --reset
