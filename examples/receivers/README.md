# Webhook receivers

pgdispatch signs every request according to the
[Standard Webhooks](https://www.standardwebhooks.com/) specification. This
directory contains small, dependency-free receivers that verify those
signatures. Copy the verification function into your own service:

| Language | File | Run |
| --- | --- | --- |
| Python 3.9+ | [`python/receiver.py`](python/receiver.py) | `WEBHOOK_SECRET=whsec_... PORT=8000 python receiver.py` |
| Node.js 18+ | [`node/receiver.js`](node/receiver.js) | `WEBHOOK_SECRET=whsec_... PORT=3000 node receiver.js` |
| Go 1.21+ | [`go/main.go`](go/main.go) | `WEBHOOK_SECRET=whsec_... PORT=8080 go run .` |

Each one reads the endpoint secret from `WEBHOOK_SECRET` and the port from
`PORT`, answers `204` for an authentic webhook and `401` otherwise.

If you would rather not maintain this code, the official
[Standard Webhooks libraries](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries)
(`pip install standardwebhooks`, `npm install standardwebhooks`, and packages
for Go, Ruby, PHP, Java, C#, Rust, Kotlin and Elixir) verify pgdispatch
webhooks out of the box.

## What a request looks like

```http
POST /webhooks HTTP/1.1
Content-Type: application/json
webhook-id: 01932c4e-8a5b-7c2d-9e1f-3a4b5c6d7e8f
webhook-timestamp: 1793528100
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"type":"payment.paid","timestamp":"2026-11-01T10:15:00.123Z","data":{"id":42}}
```

| Header | Meaning |
| --- | --- |
| `webhook-id` | The event id. It stays the same across retries, so use it to deduplicate. |
| `webhook-timestamp` | Unix time (seconds) of this delivery attempt. It is part of the signed content. |
| `webhook-signature` | One or more space-separated `v1,<base64 signature>` values. |

The body is a JSON envelope: `type` is the event type, `timestamp` is when the
event was created (ISO 8601, UTC) and `data` is the payload you published.

## Verifying a request

1. Read the three headers. Reject the request if any of them is missing.
2. Parse `webhook-timestamp` as an integer and reject the request if it is more
   than 5 minutes away from your current time, in either direction.
3. Take the secret, strip the `whsec_` prefix and base64-decode the rest. The
   resulting bytes are the HMAC key.
4. Build the signed content: `{webhook-id}.{webhook-timestamp}.{raw body}`.
5. Compute `HMAC-SHA256(key, signed content)` and base64-encode it (standard
   alphabet, with padding).
6. Split `webhook-signature` on spaces. Accept the request if any entry with
   the `v1` prefix matches your signature. Compare with a constant-time function
   (`hmac.compare_digest`, `crypto.timingSafeEqual`, `hmac.Equal`) so the
   comparison time does not leak how many bytes matched.

Several signatures appear while a secret is being rotated: pgdispatch signs
with every active secret, so a receiver keeps working whether it already has
the new secret or still has the old one.

## Things to get right

**Verify the raw body.** The signature covers the exact bytes that were sent.
Parsing the JSON and serializing it again changes whitespace, key order or
number formatting and breaks the signature. Read the raw body first, verify
it, and only then parse it. In frameworks that parse JSON automatically, use
the raw-body hook (`await request.body()` in FastAPI/Starlette,
`request.body` in Django, `express.raw()` in Express).

**Check the timestamp.** Without it, anybody who captures one request can
replay it forever. The timestamp is signed, so it cannot be changed without
invalidating the signature; a 5-minute tolerance absorbs clock drift. Keep
your server clock synchronized (NTP).

**Deduplicate by `webhook-id`.** Delivery is at-least-once: if your endpoint
times out or the connection drops after you processed a request, pgdispatch
retries it with the same `webhook-id`. Record processed ids (for example in a
table with a unique constraint, inserted in the same transaction as your side
effects) and acknowledge duplicates with `2xx` without processing them again.

**Answer quickly, process later.** Any `2xx` response marks the delivery as
successful; anything else, or a timeout, schedules a retry. Verify the
signature, store or enqueue the event, and return `2xx` immediately. Do slow
work (sending emails, calling other APIs) in a background job, so that it does
not cause timeouts and duplicate deliveries.

**Unsubscribe with `410 Gone`.** If an endpoint no longer wants webhooks, it
can answer `410`. pgdispatch then stops retrying and disables the endpoint.

**Keep the secret secret.** Load it from the environment or a secret manager,
never commit it, and rotate it if it leaks.
