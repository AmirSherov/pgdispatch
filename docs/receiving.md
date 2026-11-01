# Receiving webhooks

This page is for the people who **receive** your webhooks: your customers. You
can link to it from your own API documentation.

## The request

```http
POST /webhooks HTTP/1.1
content-type: application/json
user-agent: pgdispatch/0.1.0
webhook-id: 01a1985f-e246-725c-a639-369d5fd0a21b
webhook-timestamp: 1793527200
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"type":"order.paid","timestamp":"2026-11-01T10:00:00.123Z","data":{"id":42}}
```

| Header | Meaning |
| --- | --- |
| `webhook-id` | Unique event id. It is the same on every retry, so use it to deduplicate. |
| `webhook-timestamp` | Unix time of this attempt, in seconds. |
| `webhook-signature` | One or more space-separated `v1,<base64 signature>` values. During a secret rotation there are two. |

## Verifying the signature

The scheme is [Standard Webhooks](https://www.standardwebhooks.com/):

1. Take the secret, remove the `whsec_` prefix and base64-decode the rest. That is the HMAC key.
2. Build the signed content `"{webhook-id}.{webhook-timestamp}.{raw body}"`.
3. Compute HMAC-SHA256 with the key and base64-encode the result.
4. Compare it, in constant time, with each `v1,` value in `webhook-signature`. One match is enough.
5. Reject the request if `webhook-timestamp` is more than 5 minutes away from your clock. This blocks replayed requests.

!!! warning "Use the raw body"
    Sign and compare the **exact bytes** you received. If you parse the JSON and
    serialize it again, the bytes change and the signature no longer matches.

The official Standard Webhooks libraries do all of this for you:

=== "Python"

    ```python
    from standardwebhooks import Webhook

    payload = Webhook(secret).verify(raw_body, headers)  # raises on failure
    ```

    Or, if pgdispatch is installed:

    ```python
    from pgdispatch import verify_signature

    verify_signature(secret, headers, raw_body)
    ```

=== "Node.js"

    ```js
    import { Webhook } from "standardwebhooks";

    const payload = new Webhook(secret).verify(rawBody, headers);
    ```

=== "Go"

    ```go
    wh, _ := standardwebhooks.NewWebhook(secret)
    err := wh.Verify(rawBody, r.Header)
    ```

Libraries also exist for Ruby, PHP, Java, Kotlin, C#, Rust and Elixir. See
[the list](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries).
If you prefer no dependencies, [`examples/receivers`](https://github.com/AmirSherov/pgdispatch/tree/main/examples/receivers)
has short, complete receivers in Python, Node.js and Go.

## Receiver best practices

- **Answer quickly.** Return a 2xx status as soon as the request is verified,
  and do the work in the background. Requests time out after 15 seconds by default.
- **Deduplicate by `webhook-id`.** Delivery is at-least-once, so the same event
  can arrive twice, for example after a network failure.
- **Don't rely on order** unless the sender uses ordering keys. Use the
  `timestamp` in the body to tell which event is newer.
- **To unsubscribe, answer `410 Gone`.** The endpoint is then disabled, and
  no more requests are sent.
- **Errors are retried** with growing pauses for about 27 hours. After a long
  run of failures the endpoint is paused or disabled. The sender can resend
  everything you missed.
