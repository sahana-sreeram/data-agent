# Build from the REPO ROOT:
#   docker build -f services/application_service/Dockerfile -t application-service .
#   docker run --rm --env-file .env application-service --output s3 --num-customers 1000
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir pandas boto3 pyarrow python-dotenv
COPY src ./src
COPY services ./services

ENTRYPOINT ["python3", "-m", "services.application_service.main"]
CMD ["--output", "local"]
