"""Prometheus metrics.

Metrics are registered in ``prometheus_client``'s default registry, so they
appear on your application's existing ``/metrics`` endpoint; the standalone
worker can expose them with ``--metrics-port``. Endpoint ids are deliberately
not used as labels: thousands of endpoints would explode cardinality. Use the
CLI or the tables for per-endpoint details.
"""

from __future__ import annotations

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

__all__ = ["Metrics", "get_metrics"]

_DURATION_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0)
_BATCH_BUCKETS = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000)


class Metrics:
    """The metric objects of one registry."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.events_published = Counter(
            "pgdispatch_events_published",
            "Events published, by event type.",
            ["event_type"],
            registry=registry,
        )
        self.delivery_attempts = Counter(
            "pgdispatch_delivery_attempts",
            "HTTP delivery attempts, by result (success, http_error, timeout, network).",
            ["result"],
            registry=registry,
        )
        self.delivery_duration = Histogram(
            "pgdispatch_delivery_duration_seconds",
            "Duration of HTTP delivery attempts.",
            ["result"],
            buckets=_DURATION_BUCKETS,
            registry=registry,
        )
        self.deliveries_failed = Counter(
            "pgdispatch_deliveries_failed",
            "Deliveries that exhausted all their attempts.",
            registry=registry,
        )
        self.deliveries_pending = Gauge(
            "pgdispatch_deliveries_pending",
            "Deliveries waiting for an attempt on enabled endpoints.",
            registry=registry,
        )
        self.oldest_pending_age = Gauge(
            "pgdispatch_oldest_pending_age_seconds",
            "How long the most overdue delivery has been waiting (delivery lag).",
            registry=registry,
        )
        self.endpoints_circuit_open = Gauge(
            "pgdispatch_endpoints_circuit_open",
            "Enabled endpoints whose circuit breaker is open or half-open.",
            registry=registry,
        )
        self.worker_batch_size = Histogram(
            "pgdispatch_worker_batch_size",
            "Deliveries claimed per database round-trip.",
            buckets=_BATCH_BUCKETS,
            registry=registry,
        )


_instances: dict[int, Metrics] = {}


def get_metrics(registry: CollectorRegistry | None = None) -> Metrics:
    """Return the metrics bound to ``registry``, creating them on first use."""
    registry = registry or REGISTRY
    key = id(registry)
    if key not in _instances:
        _instances[key] = Metrics(registry)
    return _instances[key]
