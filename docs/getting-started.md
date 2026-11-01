# Getting started

## Install

=== "SQLAlchemy / FastAPI"

    ```bash
    pip install "pgdispatch[sqlalchemy,fastapi]"
    ```

=== "Django"

    ```bash
    pip install "pgdispatch[django]"
    ```

=== "asyncpg"

    ```bash
    pip install pgdispatch
    ```

You need Python 3.10 or newer and PostgreSQL 13 or newer. With Django, the
PostgreSQL version must also be one your Django release supports (Django 5.x
needs 14+, Django 6 needs 15+). The optional
`encryption` extra adds encryption of signing secrets at rest
(see [Security](security.md#signing-secrets)).

## 1. Create the schema

pgdispatch keeps its tables in a separate `pgdispatch` schema, so they never mix
with yours. Choose whichever way fits your project:

```bash
pgdispatch migrate --dsn postgresql://app:secret@localhost/app
```

```python
await webhooks.migrate()  # from code, e.g. on start-up
app = FastAPI(lifespan=webhooks.lifespan(migrate=True))
```

- **Django:** add `"pgdispatch.django"` to `INSTALLED_APPS` and run `python manage.py migrate`.
- **Alembic:** add pgdispatch to your own migration chain:

```python
from alembic import op
from pgdispatch.sqlalchemy import run_migrations


def upgrade() -> None:
    run_migrations(op.get_bind())
```

Migrations are idempotent, and an advisory lock protects them, so several
processes can run them at the same time without harm.

## 2. Create a `Webhooks` instance

```python
from pgdispatch import Webhooks, WebhooksConfig
from pgdispatch.sqlalchemy import SQLAlchemyBackend

webhooks = Webhooks(SQLAlchemyBackend(engine), WebhooksConfig(concurrency=50))
```

With plain asyncpg, use `AsyncpgBackend("postgresql://...")`. With Django, import the
ready-made instance with `from pgdispatch.django import webhooks`. It is configured
from `settings.PGDISPATCH`.

## 3. Register an endpoint

```python
endpoint = await webhooks.endpoints.create(
    "https://merchant.example.com/webhooks",
    event_types=["order.*", "refund.created"],
    owner_id="merchant_42",
)
print(endpoint.secret)  # whsec_... — give it to the receiver to verify signatures
```

## 4. Publish events

```python
async with session.begin():
    order.status = "paid"
    await webhooks.publish(session, "order.paid", {"id": order.id}, owner_id=order.merchant_id)
```

## 5. Run a worker

The worker can run inside your web process:

```python
app = FastAPI(lifespan=webhooks.lifespan())
```

It can also run as separate processes, as many as you need:

```bash
pgdispatch worker --dsn postgresql://app:secret@localhost/app
python manage.py pgdispatch_worker            # Django
```

That's all you need. To see how delivery behaves, read [Concepts](concepts.md).
To learn how receivers verify requests, read [Receiving webhooks](receiving.md).
