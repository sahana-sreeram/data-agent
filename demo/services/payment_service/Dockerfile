# Build from the REPO ROOT:
#   docker build -f demo/services/payment_service/Dockerfile -t payment-service .
#   docker run --rm --env-file .env payment-service --output s3 --contract-version v2
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir pandas boto3 pyarrow python-dotenv
COPY src ./src
COPY demo ./demo

ENTRYPOINT ["python3", "-m", "demo.services.payment_service.main"]
CMD ["--output", "local"]
