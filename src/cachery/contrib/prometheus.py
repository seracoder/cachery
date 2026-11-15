"""Prometheus metrics middleware — optional extra ``prometheus``.

Counters for hit/miss/set/error per backend, histogram for backend latency.
Import this module only when ``prometheus-client`` is installed.
"""

from __future__ import annotations

from typing import Any

from cachery.core.middleware import CacheEvent

__all__ = ["PrometheusMiddleware", "metrics_middleware"]


def _import_prometheus() -> Any:
    try:
        import prometheus_client  # noqa: PLC0415 - optional extra

        return prometheus_client
    except ImportError as e:
        raise ImportError(
            "Prometheus metrics require the 'prometheus' extra: pip install cachery[prometheus]"
        ) from e


class PrometheusMiddleware:
    def __init__(self) -> None:
        prom = _import_prometheus()
        self._hits = prom.Counter("cachery_hits_total", "Cache hits", ["backend"])
        self._misses = prom.Counter("cachery_misses_total", "Cache misses", ["backend"])
        self._sets = prom.Counter("cachery_sets_total", "Cache writes", ["backend"])
        self._errors = prom.Counter("cachery_errors_total", "Backend errors", ["backend"])
        self._latency = prom.Histogram(
            "cachery_backend_latency_seconds",
            "Backend operation latency",
            ["backend", "op"],
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
        )

    def __call__(self, event: CacheEvent) -> None:
        backend = event.backend
        if event.name == "hit":
            self._hits.labels(backend).inc()
        elif event.name == "miss":
            self._misses.labels(backend).inc()
        elif event.name == "set":
            self._sets.labels(backend).inc()
        elif event.name == "error":
            self._errors.labels(backend).inc()
        self._latency.labels(backend, event.name).observe(event.duration_ms / 1000.0)


def metrics_middleware() -> PrometheusMiddleware:
    return PrometheusMiddleware()
