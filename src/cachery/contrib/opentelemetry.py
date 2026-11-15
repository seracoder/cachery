"""OpenTelemetry tracing middleware — optional extra ``otel``.

A span per cache event (``cachery.hit`` / ``cachery.miss`` / ``cachery.set`` /
``cachery.error``) with ``cachery.backend`` and ``cachery.key`` attributes,
auto-parented under the calling span.
"""

from __future__ import annotations

from typing import Any

from cachery.core.middleware import CacheEvent

__all__ = ["OpenTelemetryMiddleware"]


class OpenTelemetryMiddleware:
    def __init__(self, tracer: Any | None = None) -> None:
        try:
            from opentelemetry import trace as otel_trace  # noqa: PLC0415 - optional extra
            from opentelemetry.trace import (  # noqa: PLC0415
                SpanKind,
                Status,
                StatusCode,
            )
        except ImportError as e:
            raise ImportError(
                "OpenTelemetry tracing requires the 'otel' extra: pip install cachery[otel]"
            ) from e
        self._tracer = tracer or otel_trace.get_tracer("cachery")
        self._SpanKind = SpanKind
        self._Status = Status
        self._StatusCode = StatusCode

    def __call__(self, event: CacheEvent) -> None:
        with self._tracer.start_as_current_span(
            f"cachery.{event.name}", kind=self._SpanKind.INTERNAL
        ) as span:
            span.set_attribute("cachery.backend", event.backend)
            if event.key is not None:
                span.set_attribute("cachery.key", event.key)
            span.set_attribute("cachery.duration_ms", event.duration_ms)
            if event.error is not None:
                span.set_status(self._Status(self._StatusCode.ERROR, event.error))
