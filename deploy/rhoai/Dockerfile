# One shared image for every RHOAI-deployed piece of this demo -- the console/API, both MCP
# servers, and the Spark driver/executors submitted via the Spark Operator all run this same
# image with a different ENTRYPOINT/command (see the Deployment/SparkApplication manifests in
# this directory). Mirrors services/*/Dockerfile's "build from repo root" convention.
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
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /opt/spark-app
COPY pyproject.toml ./
COPY src ./src
COPY static ./static
COPY context ./context
COPY pipelines ./pipelines

RUN pip install --no-cache-dir ".[rhoai]"

# Default: the console/API (overridden per-deployment -- see mcp-*-deployment.yaml and
# sparkapplication-loan-portfolio.yaml, which set their own command/mainApplicationFile).
EXPOSE 8000
ENTRYPOINT ["python3", "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
