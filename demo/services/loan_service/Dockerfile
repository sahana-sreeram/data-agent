# Build from the REPO ROOT:
#   docker build -f demo/services/loan_service/Dockerfile -t loan-service .
#   docker run --rm --env-file .env loan-service --output s3 --num-customers 1000
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir pandas boto3 pyarrow python-dotenv
COPY src ./src
COPY demo ./demo

ENTRYPOINT ["python3", "-m", "demo.services.loan_service.main"]
CMD ["--output", "local"]
