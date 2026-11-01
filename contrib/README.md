# Observability assets

Ready-made monitoring for pgdispatch. The [Docker Compose demo](../docker-compose.yml)
uses all of them.

| Path | Contents |
| --- | --- |
| [`grafana/dashboard.json`](grafana/dashboard.json) | The **pgdispatch** dashboard: delivery throughput by result, success ratio, latency percentiles, failed deliveries, delivery lag, pending deliveries, open circuit breakers, events published by type and worker batch size. |
| [`grafana/provisioning/`](grafana/provisioning) | Grafana provisioning for a Prometheus data source (uid `prometheus`) and the dashboard, as mounted by the demo. |
| [`prometheus/alerts.yml`](prometheus/alerts.yml) | Alerting rules: delivery lag above 5 minutes, more than 20% failed attempts over 10 minutes, newly opened circuit breakers, deliveries stalled although work is due, and no worker running. |
| [`prometheus/prometheus.yml`](prometheus/prometheus.yml) | The demo's scrape configuration, which loads `alerts.yml`. |

## Metrics

Applications export pgdispatch metrics on their existing `/metrics` endpoint
(they live in `prometheus_client`'s default registry); standalone workers serve
them with `pgdispatch worker --metrics-port 9100`.

| Metric | Type | Labels |
| --- | --- | --- |
| `pgdispatch_events_published_total` | counter | `event_type` |
| `pgdispatch_delivery_attempts_total` | counter | `result`: `success`, `http_error`, `timeout`, `network` |
| `pgdispatch_delivery_duration_seconds` | histogram | `result` |
| `pgdispatch_deliveries_failed_total` | counter | |
| `pgdispatch_deliveries_pending` | gauge | |
| `pgdispatch_oldest_pending_age_seconds` | gauge | |
| `pgdispatch_endpoints_circuit_open` | gauge | |
| `pgdispatch_worker_batch_size` | histogram | |

The gauges describe the shared queue and are refreshed by every worker, so
aggregate them with `max()`, not `sum()`. Processes that only publish export
them as 0.

## Using the dashboard in your Grafana

Import `grafana/dashboard.json` (*Dashboards → New → Import*). It has a
*Data source* variable, so it works with any Prometheus data source. In
`alerts.yml`, adjust the `job` label of `PgDispatchNoWorkers` to the job that
scrapes your workers.
