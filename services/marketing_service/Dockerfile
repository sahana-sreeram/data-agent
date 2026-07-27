# Build from the REPO ROOT (this service needs src/generate_data.py + services/common/):
#   docker build -f services/marketing_service/Dockerfile -t marketing-service .
#   docker run --rm --env-file .env marketing-service --output s3 --num-customers 1000
#
# Installs only what a producer actually needs (pandas/boto3/pyarrow/python-dotenv) rather
# than the full `pip install -e .` dependency set -- these are lightweight event producers,
# not the Spark/FastAPI application, and don't need PySpark or a JVM at all.
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir pandas boto3 pyarrow python-dotenv
COPY src ./src
COPY services ./services

ENTRYPOINT ["python3", "-m", "services.marketing_service.main"]
CMD ["--output", "local"]
