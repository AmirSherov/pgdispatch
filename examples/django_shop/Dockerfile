# Build from the repository root:
#   docker build -f examples/django_shop/Dockerfile -t pgdispatch-django-shop .
#
# The same image runs the web application (default command), the migrations
# (`python manage.py migrate`) and the worker (`python manage.py pgdispatch_worker`).
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# pgdispatch from this repository, instead of the released package.
COPY pyproject.toml README.md LICENSE pgdispatch/
COPY src pgdispatch/src
RUN pip install "./pgdispatch[django]" && rm -rf pgdispatch

COPY examples/django_shop/requirements.txt .
RUN pip install -r requirements.txt

COPY examples/django_shop .

RUN useradd --create-home shop
USER shop

EXPOSE 8000
CMD ["gunicorn", "shop.wsgi", "--bind", "0.0.0.0:8000", "--access-logfile", "-"]
