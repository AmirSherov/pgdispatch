# FastAPI shop

A small multi-merchant shop built with FastAPI and SQLAlchemy 2.0 (async). When
an order is created or paid, the merchant's webhook endpoints are notified.

- `POST /orders` inserts an order and publishes `order.created` **in the same
  transaction**: if the insert rolls back, no webhook is ever sent.
- `POST /orders/{id}/pay` publishes `order.paid` with an `ordering_key`, so a
  merchant never receives `order.paid` before `order.created`, and an
  `idempotency_key`, so the event is never published twice.
- `/webhooks/...` is the ready-made [`pgdispatch.fastapi`](../../src/pgdispatch/fastapi/__init__.py)
  router: merchants register endpoints, inspect deliveries and retry them.
- `/metrics/` exposes Prometheus metrics.

The whole application is [`shop/main.py`](shop/main.py) and
[`shop/models.py`](shop/models.py).

> The merchant is identified by an `X-Merchant-Id` header. That is a stand-in
> for real authentication; see `current_merchant` in `shop/main.py`.

## Five-minute walkthrough

Everything runs with Docker Compose, from the repository root:

```sh
docker compose up --build
```

This starts PostgreSQL, the shop on <http://localhost:8000> (API docs at
<http://localhost:8000/docs>), a separate `pgdispatch worker`, a webhook
[receiver](../receiver), Prometheus on <http://localhost:9090> and Grafana on
<http://localhost:3000>. Run the commands below in a second terminal.

### 1. Register an endpoint

Merchant `merchant_1` subscribes the receiver to all order events:

```sh
curl -s localhost:8000/webhooks/endpoints \
  -H 'X-Merchant-Id: merchant_1' -H 'Content-Type: application/json' \
  -d '{"url": "http://receiver:8080/webhooks", "event_types": ["order.*"]}'
```

```json
{"id": "e2ea5a5e-...", "url": "http://receiver:8080/webhooks", "circuit_state": "closed",
 "secret": "whsec_w0on+TpVVboddZ3+X72YeTi/RKqB9osaE+qXvWW5yOo=", ...}
```

The `secret` is shown only once. The receiver needs it to verify signatures;
until then it rejects every webhook with 401. Hand it over and restart the
receiver:

```sh
echo 'WEBHOOK_SECRET=whsec_w0on+TpVVboddZ3+X72YeTi/RKqB9osaE+qXvWW5yOo=' > .env
docker compose up -d receiver
```

### 2. Create and pay an order

```sh
curl -s localhost:8000/orders \
  -H 'X-Merchant-Id: merchant_1' -H 'Content-Type: application/json' \
  -d '{"customer_email": "ada@example.com", "amount_cents": 4200}'

curl -s -X POST localhost:8000/orders/1/pay -H 'X-Merchant-Id: merchant_1'
```

The worker delivers both events within milliseconds, in order:

```sh
docker compose logs receiver
```

```text
receiver-1  | INFO order.created 01a19866-0a2e-... {"id": 1, "status": "created", ...}
receiver-1  | INFO order.paid 01a19866-50cd-... {"id": 1, "status": "paid", ...}
```

### 3. Inspect the deliveries

```sh
curl -s localhost:8000/webhooks/deliveries -H 'X-Merchant-Id: merchant_1'
curl -s localhost:8000/webhooks/deliveries/1 -H 'X-Merchant-Id: merchant_1'
```

The detail view lists every HTTP attempt with its status code, response body
and duration. Merchants only ever see their own data: try the same requests
with `X-Merchant-Id: merchant_2`.

The `pgdispatch` CLI shows the same information across all merchants:

```sh
docker compose exec worker pgdispatch status
docker compose exec worker pgdispatch deliveries
docker compose exec worker pgdispatch deliveries show 1
```

### 4. Break the receiver

Make the receiver fail half of the requests with 503:

```sh
echo 'FAIL_RATE=0.5' >> .env
docker compose up -d receiver
for i in $(seq 1 50); do
  curl -s -o /dev/null localhost:8000/orders \
    -H 'X-Merchant-Id: merchant_1' -H 'Content-Type: application/json' \
    -d '{"customer_email": "load@example.com", "amount_cents": 100}'
done
```

Failed attempts are retried on a schedule (5 seconds, 5 minutes, 30 minutes, ...
up to 8 attempts over about a day). Open the **pgdispatch** dashboard in
Grafana (<http://localhost:3000>) to watch the success ratio, the retries and
the delivery lag. Deliveries can also be retried at once:

```sh
# one delivery, as a merchant
curl -s -X POST localhost:8000/webhooks/deliveries/1/retry -H 'X-Merchant-Id: merchant_1'
# every delivery that exhausted its retries, as an operator
docker compose exec worker pgdispatch retry --status failed
```

The alerting rules Prometheus evaluates are listed at
<http://localhost:9090/alerts>.

### 5. Clean up

```sh
docker compose down -v
rm .env
```

## Running the worker in-process or separately

The shop can deliver webhooks itself, from a background task started by its
lifespan handler (`RUN_WORKER=true`, the default). That is the simplest
deployment: one process, nothing else to run.

In Docker Compose the shop runs with `RUN_WORKER=false` and the `worker`
service runs `pgdispatch worker` from the same image instead, so delivery
scales independently of the web application:

```sh
docker compose up -d --scale worker=3
```

Workers coordinate through PostgreSQL (`FOR UPDATE SKIP LOCKED`), so any number
of them, in-process or standalone, can run side by side.

## Running without Docker

You need PostgreSQL and Python 3.10+:

```sh
cd examples/fastapi_shop
pip install -e "../..[sqlalchemy,fastapi]"  # pgdispatch from this clone
pip install -r requirements.txt
export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/postgres
# Only for local receivers: allow http:// URLs and private addresses.
export PGDISPATCH_ALLOW_HTTP=true PGDISPATCH_ALLOW_PRIVATE_IPS=true
uvicorn shop.main:app --reload
```

and, for a receiver, in another terminal:

```sh
WEBHOOK_SECRET=whsec_... python ../receiver/receiver.py
```
