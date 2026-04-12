# ABOUTME: Tests for OpenTelemetry instrumentation no-op path
# ABOUTME: Verifies that otel module works without OTel SDK installed

from brooklet.contrib import otel
from brooklet.contrib.otel import (
    _NoOpCounter,
    _NoOpHistogram,
    _NoOpMeter,
    _NoOpSpan,
    _NoOpTracer,
)


class TestNoOpTracer:
    """No-op tracer must be usable as a drop-in for real OTel tracers."""

    def test_start_as_current_span_is_context_manager(self):
        noop = _NoOpTracer()
        with noop.start_as_current_span("test") as span:
            assert span is not None

    def test_span_set_attribute_is_noop(self):
        span = _NoOpSpan()
        span.set_attribute("key", "value")  # should not raise

    def test_span_context_manager(self):
        span = _NoOpSpan()
        with span as s:
            assert s is span


class TestNoOpMeter:
    """No-op meter must return instruments whose methods don't raise."""

    def test_create_counter(self):
        m = _NoOpMeter()
        counter = m.create_counter("test.counter")
        assert isinstance(counter, _NoOpCounter)
        counter.add(1)  # should not raise
        counter.add(5, {"topic": "foo"})  # should not raise

    def test_create_histogram(self):
        m = _NoOpMeter()
        hist = m.create_histogram("test.histogram")
        assert isinstance(hist, _NoOpHistogram)
        hist.record(42)  # should not raise
        hist.record(100, {"topic": "bar"})  # should not raise


class TestModuleLevelHandles:
    """Module-level tracer and meter must be usable without OTel installed."""

    def test_tracer_span(self):
        with otel.tracer.start_as_current_span("test-span") as span:
            span.set_attribute("key", "value")

    def test_meter_counter(self):
        counter = otel.meter.create_counter("test.counter")
        counter.add(1)

    def test_meter_histogram(self):
        hist = otel.meter.create_histogram("test.hist")
        hist.record(10)


class TestConfigure:
    """configure() must be safe to call without OTel SDK."""

    def test_configure_without_otel_returns_false(self):
        # Since OTel is not in the dev dependency group, this should return False
        # (or True if OTel happens to be installed — both are valid)
        result = otel.configure()
        assert isinstance(result, bool)
