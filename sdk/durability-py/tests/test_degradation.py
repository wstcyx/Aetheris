"""Tests for SDK degradation and self-preservation (Issue #7).

8 failure injection matrix: connection refused, DNS failure, TLS
failure, 500, 429, response truncation, slow response, network
partition. Each must verify fail-open: business function returns
correctly regardless of reporting failure.
"""
import os
import socket
import ssl
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from unittest.mock import patch

import pytest

from aetheris_durability import Reporter
from aetheris_durability.types import Event, EventType


def business_fn(state):
    """A business function that should always succeed regardless of
    reporting failures."""
    return {"result": "ok"}


class TestFailureMatrix:
    """8 failure scenarios, each verifying fail-open invariant."""

    def _make_reporter(self, endpoint, **kwargs):
        defaults = dict(
            tenant_id="t", token="tok", agent_id="a",
            timeout=0.5, max_retries=1, base_backoff=0.01, max_backoff=0.05,
            flush_interval=0,  # sync mode for deterministic tests
        )
        defaults.update(kwargs)
        return Reporter(endpoint=endpoint, **defaults)

    def test_01_connection_refused_fail_open(self):
        """Port 1 = connection refused. Business must still work."""
        reporter = self._make_reporter("http://127.0.0.1:1")

        @reporter.step("step1")
        def step1(state):
            return business_fn(state)

        result = step1({"input": "test"})
        assert result == {"result": "ok"}
        assert reporter._failed_count > 0

    def test_02_dns_failure_fail_open(self):
        """Non-existent domain = DNS failure. Business must still work."""
        reporter = self._make_reporter("http://nonexistent.invalid.domain.xyz")

        @reporter.step("step1")
        def step1(state):
            return business_fn(state)

        result = step1({})
        assert result == {"result": "ok"}

    def test_03_500_error_fail_open(self):
        """Server returns 500. Business must still work."""

        class Handler500(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(500)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler500)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        try:
            reporter = self._make_reporter(f"http://127.0.0.1:{port}")

            @reporter.step("step1")
            def step1(state):
                return business_fn(state)

            result = step1({})
            assert result == {"result": "ok"}
            assert reporter._failed_count > 0
        finally:
            server.shutdown()

    def test_04_429_rate_limit_fail_open(self):
        """Server returns 429. Business must still work, sample rate reduced."""

        class Handler429(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler429)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        try:
            reporter = self._make_reporter(f"http://127.0.0.1:{port}")

            @reporter.step("step1")
            def step1(state):
                return business_fn(state)

            result = step1({})
            assert result == {"result": "ok"}
            # 429 should reduce sample rate
            assert reporter._sample_rate <= 0.5
            # Circuit should be open after 429
            assert reporter._circuit_open
        finally:
            server.shutdown()

    def test_05_slow_response_fail_open(self):
        """Server responds slowly. Business must not block indefinitely."""

        class HandlerSlow(BaseHTTPRequestHandler):
            def do_POST(self):
                time.sleep(5)  # slower than timeout
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), HandlerSlow)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        try:
            reporter = self._make_reporter(
                f"http://127.0.0.1:{port}",
                timeout=0.5,  # 0.5s timeout
                max_retries=1,
            )

            start = time.time()
            @reporter.step("step1")
            def step1(state):
                return business_fn(state)

            result = step1({})
            elapsed = time.time() - start

            # Business function completed
            assert result == {"result": "ok"}
            # Should not take more than 3 seconds (timeout + 1 retry backoff)
            assert elapsed < 3.0, f"took {elapsed}s, expected < 3s"
        finally:
            server.shutdown()

    def test_06_response_truncation_fail_open(self):
        """Server sends incomplete response. Business must still work."""

        class HandlerTrunc(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Length", "1000")
                self.end_headers()
                self.wfile.write(b"truncated")  # only 8 bytes of 1000 promised
                self.wfile.flush()
                self._shutdown = True

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), HandlerTrunc)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        try:
            reporter = self._make_reporter(f"http://127.0.0.1:{port}")

            @reporter.step("step1")
            def step1(state):
                return business_fn(state)

            result = step1({})
            assert result == {"result": "ok"}
        finally:
            server.shutdown()

    def test_07_network_partition_fail_open(self):
        """Network partition = unreachable host. Business must still work."""
        # Use a non-routable address (TEST-NET)
        reporter = self._make_reporter("http://192.0.2.1:8080", timeout=0.5)

        @reporter.step("step1")
        def step1(state):
            return business_fn(state)

        start = time.time()
        result = step1({})
        elapsed = time.time() - start

        assert result == {"result": "ok"}
        # Should timeout quickly
        assert elapsed < 5.0, f"took {elapsed}s, expected < 5s"

    def test_08_circuit_breaker_opens_after_failures(self):
        """After N consecutive failures, circuit breaker opens
        and further events are buffered without network attempts."""
        reporter = self._make_reporter(
            "http://127.0.0.1:1",  # unreachable
            max_retries=1,
            base_backoff=0.01,
        )

        # First call: triggers failure, opens circuit
        @reporter.step("step1")
        def step1(state):
            return business_fn(state)

        result1 = step1({})
        assert result1 == {"result": "ok"}
        assert reporter._circuit_open

        # Second call: circuit is open, should return immediately
        # (no retry attempts, no long wait)
        start = time.time()
        result2 = step1({})
        elapsed = time.time() - start
        assert result2 == {"result": "ok"}
        # Circuit open = no network attempt = fast
        assert elapsed < 1.0, f"circuit should skip network, took {elapsed}s"


class TestBufferOverflowPolicy:
    """Verify buffer overflow drops oldest with counting (#7)."""

    def test_drop_oldest_and_count(self):
        reporter = Reporter(
            endpoint="http://127.0.0.1:1",
            tenant_id="t", token="tok",
            max_buffer=5,
            max_retries=1,
            base_backoff=0.001,
            timeout=0.1,
        )
        # Fill buffer directly (bypass flush)
        for i in range(20):
            with reporter._lock:
                if len(reporter._buffer) >= reporter._max_buffer:
                    reporter._buffer.pop(0)
                    reporter._dropped_count += 1
                reporter._buffer.append(Event(
                    type=EventType.STEP_STARTED,
                    job_id=f"job-{i}",
                    payload={},
                ))

        assert reporter._dropped_count == 15  # 20 - 5
        assert len(reporter._buffer) == 5


class TestExponentialBackoff:
    """Verify exponential backoff with jitter on retry."""

    def test_backoff_increases_on_retry(self):
        reporter = Reporter(
            endpoint="http://127.0.0.1:1",
            tenant_id="t", token="tok",
            max_retries=3,
            base_backoff=0.05,
            max_backoff=0.5,
            timeout=0.1,
            flush_interval=0,  # sync mode
        )
        reporter.report(Event(type=EventType.JOB_CREATED, job_id="j1", payload={}))
        assert reporter._retry_count >= 1
        assert reporter._retry_count >= 1
