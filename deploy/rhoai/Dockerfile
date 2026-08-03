# One shared image for every RHOAI-deployed piece of this demo -- the console/API, both MCP
# servers, and the Spark driver/executors submitted via the Spark Operator all run this same
# image with a different ENTRYPOINT/command (see the Deployment/SparkApplication manifests in
# this directory). Mirrors demo/services/*/Dockerfile's "build from repo root" convention.
#
# Build from the REPO ROOT:
#   docker build -f deploy/rhoai/Dockerfile -t <registry>/data-agent:latest .
#
# NOT built or pushed by anything in this repo -- see deploy/rhoai/RUNBOOK.md for the actual
# build/push commands, run only after cluster access is confirmed (this demo's explicit
# "do not build or publish images until authenticated" boundary).
FROM python:3.12-slim

# Java is required for PySpark (the console/API and MCP servers import src.spark_session
# transitively; the Spark Operator's own image would normally bundle Spark, but for this
# demo's scale a plain PySpark install on the same base image is sufficient).
# openjdk-17 is unavailable on this base image's current Debian release (trixie) -- confirmed
# live via an on-cluster build failure ("Package 'openjdk-17-jre-headless' has no installation
# candidate"); openjdk-21 is what trixie actually ships, and recent PySpark releases support it.
# git is required by src.pr_artifact/src.sandbox.backend -- verify_candidate_repair's
# create_pr path builds a real throwaway git branch/commit for the PR-ready artifact
# (confirmed live: "[Errno 2] No such file or directory: 'git'" without this).
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl git && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /opt/spark-app
COPY pyproject.toml ./
COPY src ./src
COPY demo ./demo
COPY static ./static
COPY context ./context
COPY pipelines ./pipelines
# lifecycle_verify_repair._run_pytest_against_patched_code runs
# tests/test_etl_spark_<pipeline>.py as a real, necessary part of verify_candidate_repair --
# not just this repo's own test suite -- so it must ship in the image, not be dev-only.
COPY tests ./tests
# src.pr_artifact's create_pr path needs a REAL git history to create a real throwaway
# branch/commit from (GitWorktreeSandbox's `git worktree add -b <branch> <dir> HEAD` requires
# repo_root="." -- this image's own WORKDIR -- to already be a real git repo with a HEAD
# commit, confirmed live: without this, `git worktree add` fails with "not a git repository").
COPY .git ./.git
# Confirmed live: OpenShift's arbitrary non-root UID needs group write access here too, same
# reasoning as the context/ chmod below -- git worktree/commit both write into .git/.
RUN chmod -R g+w /opt/spark-app/.git
# A fresh container has no git identity configured -- `git commit` fails outright ("Please
# tell me who you are") without one. This identity only ever appears on throwaway, unpushed
# candidate-repair branches (never on real commits a human authored). --system (not --global)
# because the runtime container's HOME (/tmp, see configmap-app-config.yaml) differs from the
# build-time root user's HOME (~/.gitconfig would be written somewhere --global never finds
# again) -- /etc/gitconfig is a fixed, always-found location regardless of HOME or UID.
# safe.directory is required too: git refuses to operate on a repo owned by a different UID
# than the current process by default, and this image's files are owned by root (build time)
# while the runtime process is an arbitrary non-root UID (confirmed live pattern throughout
# this file).
RUN git config --system user.name "data-agent-bot" && \
    git config --system user.email "data-agent-bot@localhost" && \
    git config --system --add safe.directory /opt/spark-app

# Confirmed live: OpenShift runs this image as an arbitrary, non-root UID (in group 0/root,
# the standard OpenShift convention) -- context/context_enrichment's regeneration path
# (ContextRetriever.ensure_fresh -> FileContextStore.save_generated_context) writes to
# context/generated/<pipeline>.json on disk, which fails with a real Permission denied unless
# the group (0, which every arbitrary UID belongs to under OpenShift) has write access.
RUN chmod -R g+w /opt/spark-app/context

RUN pip install --no-cache-dir ".[rhoai]"

# S3A support (org.apache.hadoop:hadoop-aws + its AWS SDK) is resolved at runtime via
# spark.jars.packages (src.spark_session.get_spark_session), NOT baked in here -- this pod
# (unlike the Spark Operator's own controller pod, or the driver/executor pods built from
# Dockerfile.spark) is a plain process where get_spark_session() runs local-mode Spark
# directly, so PySpark's own Ivy-based package resolution actually applies and works here
# (confirmed live, once deploy/rhoai/configmap-app-config.yaml's HOME=/tmp fix made its cache
# directory writable) -- letting Ivy resolve the full, correct, VERSION-MATCHED transitive
# dependency closure (confirmed live: manually guessing and baking just hadoop-aws + one SDK
# jar missed a real transitive dependency, software.amazon.s3.analyticsaccelerator, that
# Hadoop 3.5's S3A implementation requires unconditionally).

# Default: the console/API (overridden per-deployment -- see mcp-*-deployment.yaml and
# sparkapplication-loan-portfolio.yaml, which set their own command/mainApplicationFile).
EXPOSE 8000
ENTRYPOINT ["python3", "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
