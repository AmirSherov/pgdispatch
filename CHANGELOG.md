# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-11-01

The first public release.

### Added

- Transactional outbox: `Webhooks.publish()` joins the caller's transaction (asyncpg, SQLAlchemy 2.0 async, Django).
- Fan-out to subscribed endpoints with `*`, exact and `prefix.*` patterns, isolated by `owner_id`.
- Idempotent publishing (`idempotency_key`) and ordered delivery (`ordering_key`).
- Delivery worker: claiming with `FOR UPDATE SKIP LOCKED` and leases, `LISTEN/NOTIFY` wake-ups, batched result recording, per-endpoint concurrency limits and rate limits, graceful shutdown.
- Standard Webhooks signatures; secret rotation with a grace period; optional Fernet encryption of secrets at rest.
- Retry schedule with jitter and `Retry-After`; per-endpoint circuit breaker; automatic disabling of endpoints that keep failing; `410 Gone` handling.
- SSRF protection: URL validation, a resolver that pins connections to checked public addresses, no redirects.
- Management API for endpoints, deliveries (list, attempts, retry, cancel) and events (list, replay); test events.
- FastAPI router for customer-facing endpoint management; ASGI lifespan integration.
- Django app with migrations, a synchronous facade and the `pgdispatch_worker` command; an Alembic helper.
- `pgdispatch` CLI: `migrate`, `worker`, `status`, `deliveries`, `retry`, `replay`, `endpoints`, `cleanup`.
- Prometheus metrics, a Grafana dashboard, alert rules, structured logs and hooks.
- Retention clean-up of old events, deliveries and attempts.

[Unreleased]: https://github.com/AmirSherov/pgdispatch/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AmirSherov/pgdispatch/releases/tag/v0.1.0
