#!/usr/bin/env bash
# One-command setup: MinIO up, dependencies installed, data generated and migrated,
# ETL pipelines run. Safe to rerun -- every step is idempotent (existing .env is never
# overwritten, docker compose up is a no-op if MinIO is already running, and
# generate_data/migrate_lifecycle_to_s3/run_lifecycle_etl_pipelines all regenerate the
# same deterministic output).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> Checking .env"
if [ ! -f .env ]; then
  cp .env.example .env
  echo "    Created .env from .env.example -- fill in OPENAI_API_KEY and JAVA_HOME before continuing."
else
  echo "    .env already exists, leaving it as-is."
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

if [ -z "${OPENAI_API_KEY:-}" ]; then
  echo "!! OPENAI_API_KEY is not set in .env -- diagnosis/repair/Q&A calls will fail until it is." >&2
fi

if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  echo "!! JAVA_HOME is not set (or doesn't point at a real JDK) in .env." >&2
  echo "   Install one and point JAVA_HOME at it, e.g.: brew install openjdk@17" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "!! docker is required (for MinIO) but was not found on PATH." >&2
  exit 1
fi

echo "==> Starting MinIO (docker compose up -d)"
docker compose up -d

echo "==> Waiting for MinIO to become healthy"
for _ in $(seq 1 30); do
  status="$(docker inspect --format '{{.State.Health.Status}}' data-agent-minio 2>/dev/null || echo "starting")"
  if [ "$status" = "healthy" ]; then
    echo "    MinIO is healthy."
    break
  fi
  sleep 2
done
if [ "$status" != "healthy" ]; then
  echo "!! MinIO did not become healthy in time. Check 'docker compose logs minio'." >&2
  exit 1
fi

echo "==> Installing Python dependencies"
if command -v uv >/dev/null 2>&1; then
  uv pip install -e .
else
  python3 -m pip install -e .
fi

echo "==> Generating synthetic lifecycle data"
python3 -m src.generate_data --output-dir data/lifecycle/raw

echo "==> Migrating data + context to S3-compatible storage"
python3 -m src.migrate_lifecycle_to_s3

echo "==> Running the 5 curated ETL pipelines"
python3 -m src.run_lifecycle_etl_pipelines

echo "==> Done. Start the app with: python3 -m src.api"
