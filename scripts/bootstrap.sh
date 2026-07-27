#!/usr/bin/env bash
# One-command setup: MinIO up, dependencies installed, data generated and migrated,
# ETL pipelines run. Safe to rerun -- every step is idempotent (existing .env is never
# overwritten, MinIO startup is skipped if it's already reachable, and
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

minio_is_reachable() {
  curl -sf -o /dev/null "http://localhost:9000/minio/health/live"
}

echo "==> Starting MinIO"
if minio_is_reachable; then
  # Checks real reachability, not `docker inspect`'s Health field -- a data-agent-minio
  # container started by hand (e.g. `docker run`, no --health-cmd) never gets a Health
  # status at all, which would otherwise make this script think MinIO is down and either
  # collide with `docker compose up -d` (container_name conflict) or hang in the wait loop
  # below forever.
  echo "    MinIO is already reachable on :9000 -- skipping docker compose up."
else
  echo "    (docker compose up -d)"
  docker compose up -d
fi

echo "==> Waiting for MinIO to become reachable"
ready=""
for _ in $(seq 1 30); do
  if minio_is_reachable; then
    ready=1
    echo "    MinIO is reachable."
    break
  fi
  sleep 2
done
if [ -z "$ready" ]; then
  echo "!! MinIO did not become reachable in time. Check 'docker compose logs minio'." >&2
  exit 1
fi

echo "==> Installing Python dependencies"
if [ -n "${VIRTUAL_ENV:-}" ] && command -v uv >/dev/null 2>&1; then
  # uv pip install refuses to target a non-venv interpreter without --system -- only use it
  # when a venv is actually active; otherwise fall back to the system python3 -m pip below
  # rather than force-installing into whatever interpreter uv would otherwise pick.
  uv pip install -e .
else
  python3 -m pip install -e .
fi

echo "==> Generating synthetic lifecycle data"
python3 -m src.generate_data --output-dir data/lifecycle/raw

echo "==> Migrating data + context to S3-compatible storage"
python3 -m src.migrate_lifecycle_to_s3

echo "==> Running the curated ETL pipelines"
python3 -m src.run_lifecycle_etl_pipelines

echo "==> Done. Start the app with: python3 -m src.api"
