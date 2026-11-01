# Security

## Request signatures

Every request is signed with HMAC-SHA256 according to
[Standard Webhooks](https://www.standardwebhooks.com/). Receivers verify
both where the request came from and that it was not changed. The signature
covers a timestamp, which lets receivers reject replayed requests. See
[Receiving webhooks](receiving.md).

## Signing secrets

- Secrets look like `whsec_<base64 of 32 random bytes>` and are generated with `secrets.token_bytes`.
- A secret is returned only by `create`, `rotate_secret` and `get_secret`.
  It never appears in `repr()`, logs or metrics.
- The worker needs the plaintext secret to sign requests, so secrets cannot be
  hashed. They can be **encrypted at rest**:

```bash
pip install "pgdispatch[encryption]"
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
export PGDISPATCH_SECRET_ENCRYPTION_KEYS='["<key>"]'
```

The first key encrypts new secrets, and every key in the list can decrypt. To
rotate the encryption key, put a new key first and keep the old one until every
endpoint secret has been rotated. Secrets stored without encryption keep
working, so you can turn encryption on at any time. You can also pass any
object with `encrypt`/`decrypt` methods as `Webhooks(secret_cipher=...)`.

- Rotate a secret with zero downtime using `rotate_secret(grace_period=...)`.
  To revoke a leaked secret at once, pass `grace_period=timedelta(0)`.

## Server-side request forgery (SSRF)

Your customers choose the endpoint URLs. Without protection, a customer could
point your worker at `http://169.254.169.254/` (cloud credentials),
`http://localhost:5432` or an internal admin panel. pgdispatch defends against
this at every step:

| When | Check |
| --- | --- |
| Endpoint created or updated | `https` only, no credentials in the URL, no private IP literals, no `localhost`/`.local`/`.internal` names. |
| Every connection | The host name is resolved, and **every** resulting address must be public. Private, loopback, link-local, CGNAT, multicast and reserved IPv4 and IPv6 ranges are rejected, including IPv4-mapped, 6to4 and Teredo forms. |
| Every connection | The socket connects to the address that was checked. DNS is not looked up again, which closes the DNS-rebinding hole. TLS still checks the certificate for the original host name. |
| Every response | Redirects are never followed. |

Blocked attempts are logged with an error starting with `blocked:`. Proxy
settings from the environment are ignored, because they would bypass these
checks. To route traffic through an egress proxy on purpose, set
`outbound_proxy`. Address filtering then becomes the proxy's job.

For local development, `allow_http=True` and `allow_private_ips=True` turn
these checks off. **Never enable them in production.**

## Data and retention

- Event payloads, response bodies (up to 4 KB) and request headers are kept
  for `retention_days` (14 by default). Older data is deleted automatically.
  Mention this in your privacy policy and GDPR records.
- Set `store_response_body=False` if receivers may return sensitive data.
- Values of custom endpoint headers, such as API keys, are replaced with
  `[redacted]` in the attempt log.
- Payloads are not encrypted at rest. Leave secrets out of them, or encrypt the
  database storage.

## Reporting a vulnerability

Please report vulnerabilities privately, as described in
[SECURITY.md](https://github.com/AmirSherov/pgdispatch/blob/main/SECURITY.md).
