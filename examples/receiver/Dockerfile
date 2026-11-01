# Build from the repository root:
#   docker build -f examples/receiver/Dockerfile -t pgdispatch-receiver .
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Only `pgdispatch.verify_signature` is needed; a receiver in production would
# rather use any Standard Webhooks SDK, or the few lines of code it takes.
COPY pyproject.toml README.md LICENSE pgdispatch/
COPY src pgdispatch/src
RUN pip install ./pgdispatch && rm -rf pgdispatch

COPY examples/receiver/receiver.py .

RUN useradd --create-home receiver
USER receiver

EXPOSE 8080
CMD ["python", "receiver.py", "--port", "8080"]
