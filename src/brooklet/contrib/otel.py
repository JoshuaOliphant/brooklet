# ABOUTME: OpenTelemetry instrumentation for brooklet — optional tracing and metrics
# ABOUTME: No-op when OTel SDK not installed; zero runtime cost without the dependency

from __future__ import annotations

import logging
from contextlib import contextmanager

_logger = logging.getLogger("brooklet")

_OTEL_AVAILABLE = False
_metrics_api = None
_trace_api = None

try:
    from opentelemetry import metrics as _metrics_api
    from opentelemetry import trace as _trace_api
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover — only hit when OTel SDK is absent
    pass


# ---------------------------------------------------------------------------
# No-op wrappers for when OTel is not installed
# ---------------------------------------------------------------------------


class _NoOpSpan:
    """Minimal span stand-in that supports ``with`` and attribute setting."""

    def set_attribute(self, key: str, value: object) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _NoOpTracer:
    """Tracer that returns no-op spans."""

    @contextmanager
    def start_as_current_span(self, name: str, **kwargs):
        yield _NoOpSpan()


class _NoOpCounter:
    def add(self, amount: int | float, attributes: dict | None = None) -> None:
        pass


class _NoOpHistogram:
    def record(self, amount: int | float, attributes: dict | None = None) -> None:
        pass


class _NoOpMeter:
    """Meter that returns no-op instruments."""

    def create_counter(self, name: str, **kwargs) -> _NoOpCounter:
        return _NoOpCounter()

    def create_histogram(self, name: str, **kwargs) -> _NoOpHistogram:
        return _NoOpHistogram()


# ---------------------------------------------------------------------------
# Module-level tracer / meter — used by brooklet core modules
# ---------------------------------------------------------------------------

_configured = False


def _make_tracer():
    if _OTEL_AVAILABLE:
        return _trace_api.get_tracer("brooklet")
    return _NoOpTracer()


def _make_meter():
    if _OTEL_AVAILABLE:
        return _metrics_api.get_meter("brooklet")
    return _NoOpMeter()


tracer = _make_tracer()
meter = _make_meter()


# ---------------------------------------------------------------------------
# Configuration — call once to wire up OTLP HTTP exporters
# ---------------------------------------------------------------------------


def configure(endpoint: str = "http://127.0.0.1:4318") -> bool:
    """Set up TracerProvider + MeterProvider exporting to an OTLP HTTP endpoint.

    Returns True if OTel was configured, False if the SDK is not installed.
    Safe to call multiple times — only the first call takes effect.
    """
    global tracer, meter, _configured  # noqa: PLW0603

    if not _OTEL_AVAILABLE:
        _logger.debug(
            "OTel SDK not available — instrumentation disabled. Install with: uv sync --group otel"
        )
        return False

    if _configured:
        return True

    resource = Resource.create({"service.name": "brooklet"})

    # Traces
    span_exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(span_exporter))
    _trace_api.set_tracer_provider(tp)

    # Metrics
    metric_exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics")
    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=10000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    _metrics_api.set_meter_provider(mp)

    # Refresh module-level handles so callers pick up the real providers
    tracer = _trace_api.get_tracer("brooklet")
    meter = _metrics_api.get_meter("brooklet")

    _configured = True
    return True
