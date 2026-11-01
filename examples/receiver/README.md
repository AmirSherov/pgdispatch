# Webhook receiver

A tiny [aiohttp](https://docs.aiohttp.org/) service that stands in for a
merchant's server: it verifies the signature of every webhook with
`pgdispatch.verify_signature`, logs the event and answers 200. The Docker
Compose demo sends all webhooks here.

```sh
pip install pgdispatch
WEBHOOK_SECRET=whsec_... python receiver.py --port 8080
```

| Variable | Meaning |
| --- | --- |
| `WEBHOOK_SECRET` | The endpoint's signing secret. Without it every request is rejected with 401. |
| `FAIL_RATE` | Fraction of requests, from 0 to 1, answered with 503 to make pgdispatch retry. Default 0. |

pgdispatch signs requests according to [Standard Webhooks](https://www.standardwebhooks.com/),
so receivers written in other languages can use any Standard Webhooks library.
The essential steps, all in [`receiver.py`](receiver.py):

1. Read the raw request body; parsing and re-serializing JSON breaks the signature.
2. Verify the `webhook-signature` header against the body, `webhook-id` and
   `webhook-timestamp`, and reject timestamps older than a few minutes.
3. Deduplicate by `webhook-id`: it stays the same across retries.
4. Answer quickly with a 2xx status; do slow work in the background.
