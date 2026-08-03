#!/usr/bin/env bash
# Injects the flagship demo incident against the cluster's MinIO: payment_service renames a
# successfully collected installment's payment_status from PAID to SETTLED, but
# loan_portfolio's ETL still only recognizes PAID -- the Spark job completes successfully,
# but total_outstanding_principal becomes silently wrong. Idempotent: safe to run again if the
# incident is already injected.
#
# Requires: `oc` logged into the target cluster, and MINIO_ACCESS_KEY/MINIO_SECRET_KEY already
# exported in your shell (never hardcode these -- see deploy/rhoai/RUNBOOK.md step 4/5).
#
# Reset afterward with demo/scripts/reset-bug.sh.
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
python3 -c "
from src.storage import S3Storage
from demo.enterprise_incident import inject_contract_change
print(inject_contract_change(S3Storage(), None))
"
