"""Settings of the Django example shop; everything that varies comes from the environment."""

import os
from urllib.parse import unquote, urlsplit

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "insecure-example-key")
DEBUG = os.environ.get("DJANGO_DEBUG", "false").lower() == "true"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "pgdispatch.django",  # the pgdispatch tables, the `pgdispatch_worker` command
    "orders",
]
MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
ROOT_URLCONF = "shop.urls"
WSGI_APPLICATION = "shop.wsgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True


def _database(url: str) -> dict[str, object]:
    """Translate ``postgresql://user:password@host:port/name`` into a ``DATABASES`` entry."""
    parts = urlsplit(url)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parts.path.lstrip("/"),
        "USER": unquote(parts.username or ""),
        "PASSWORD": unquote(parts.password or ""),
        "HOST": parts.hostname or "",
        "PORT": parts.port or "",
    }


DATABASES = {
    "default": _database(
        os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    )
}

# Any pgdispatch.WebhooksConfig field; PGDISPATCH_* environment variables work too.
PGDISPATCH = {
    # The demo receiver is reached over plain HTTP on a private network.
    # Never allow either in production.
    "allow_http": DEBUG,
    "allow_private_ips": DEBUG,
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}
