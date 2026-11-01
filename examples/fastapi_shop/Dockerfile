# Build from the repository root:
#   docker build -f examples/fastapi_shop/Dockerfile -t pgdispatch-fastapi-shop .
#
# The same image runs the web application (default command) and the
# standalone worker (`pgdispatch worker`).
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# pgdispatch from this repository, instead of the released package.
COPY pyproject.toml README.md LICENSE pgdispatch/
COPY src pgdispatch/src
RUN pip install "./pgdispatch[sqlalchemy,fastapi]" && rm -rf pgdispatch

COPY examples/fastapi_shop/requirements.txt .
RUN pip install -r requirements.txt

COPY examples/fastapi_shop/shop shop

RUN useradd --create-home shop
USER shop

EXPOSE 8000
CMD ["uvicorn", "shop.main:app", "--host", "0.0.0.0", "--port", "8000"]
