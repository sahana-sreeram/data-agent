# Build from the REPO ROOT:
#   docker build -f services/underwriting_service/Dockerfile -t underwriting-service .
#   docker run --rm --env-file .env underwriting-service --output s3 --num-customers 1000
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir pandas boto3 pyarrow python-dotenv
COPY src ./src
COPY services ./services

ENTRYPOINT ["python3", "-m", "services.underwriting_service.main"]
CMD ["--output", "local"]
