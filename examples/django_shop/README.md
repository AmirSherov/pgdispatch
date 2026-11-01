# Django shop

A minimal Django project that sends webhooks with pgdispatch. Creating or
shipping an order publishes an event inside the same `transaction.atomic()`
block as the database write, and `python manage.py pgdispatch_worker` delivers
it.

| File | What it shows |
| --- | --- |
| [`shop/settings.py`](shop/settings.py) | `"pgdispatch.django"` in `INSTALLED_APPS` and the `PGDISPATCH` settings |
| [`orders/views.py`](orders/views.py) | `webhooks.publish(...)` with an `ordering_key` and an `idempotency_key` |
| [`orders/management/commands/create_endpoint.py`](orders/management/commands/create_endpoint.py) | registering an endpoint with `webhooks.endpoints.create(...)` |

`python manage.py migrate` creates the pgdispatch tables along with the
project's own.

> The merchant is identified by an `X-Merchant-Id` header. That is a stand-in
> for real authentication; see `merchant_api` in `orders/views.py`.

## Walkthrough

From the repository root, start the demo stack with the `django` profile:

```sh
docker compose --profile django up --build
```

Next to the [FastAPI demo](../fastapi_shop), this runs the Django shop on
<http://localhost:8001> and its worker. Both shops share one database, and so
one pgdispatch queue: whichever worker is free delivers the webhook, no matter
which framework published it.

Register the [receiver](../receiver) for merchant `merchant_2`, with the secret
the receiver already knows (any `whsec_` secret works; omit `--secret` to get a
random one):

```sh
docker compose exec django-shop python manage.py create_endpoint \
  http://receiver:8080/django --owner merchant_2 --events 'order.*' \
  --secret "$(grep WEBHOOK_SECRET .env | cut -d= -f2-)"
```

If you have not followed the FastAPI walkthrough, there is no `.env` yet: run
the command without `--secret`, then put the printed secret in `.env` as
`WEBHOOK_SECRET=...` and restart the receiver with `docker compose up -d receiver`.

Create an order and ship it:

```sh
curl -s localhost:8001/orders -H 'X-Merchant-Id: merchant_2' \
  -d '{"customer_email": "grace@example.com", "amount_cents": 1999}'

curl -s -X POST localhost:8001/orders/1/ship -H 'X-Merchant-Id: merchant_2'

docker compose logs receiver
```

```text
receiver-1  | INFO order.created 01a19868-6dba-... {"id": 1, "status": "created", ...}
receiver-1  | INFO order.shipped 01a19868-6e2d-... {"id": 1, "status": "shipped", ...}
```

## Running without Docker

You need PostgreSQL and Python 3.10+:

```sh
cd examples/django_shop
pip install -e "../..[django]"  # pgdispatch from this clone
pip install -r requirements.txt
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
export DJANGO_DEBUG=true  # also allows http:// and private-network endpoints
python manage.py migrate
python manage.py runserver 8001
```

and, in another terminal, the worker:

```sh
python manage.py pgdispatch_worker --metrics-port 9100
```
