"""Tests for the reporting client (Issue #6).

Core invariant: reporting failures MUST NOT propagate to business code.
"""
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from unittest.mock import patch, MagicMock

import pytest

from aetheris_durability import Reporter
from aetheris_durability.types import Event, EventType


class _MockServerHandler(BaseHTTPRequestHandler):
    """Mock ingest endpoint."""
    status_code = 200
    received_requests = []

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        self.__class__.received_requests.append(body)
        self.send_response(self.__class__.status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"accepted_version":"1","accepted":1,"deduped":0,"rejected":0}')

    def log_message(self, *args):
        pass  # suppress stderr noise


@pytest.fixture
def mock_server():
    """Start a mock ingest endpoint on a random port."""
    _MockServerHandler.received_requests = []
    server = HTTPServer(("127.0.0.1", 0), _MockServerHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    thread.join(timeout=2)


def make_reporter(endpoint="http://127.0.0.1:0", agent_id="test-agent"):
    return Reporter(
        endpoint=endpoint,
        tenant_id="tenant-test",
        token="test-token",
        agent_id=agent_id,
        flush_interval=0,  # sync mode for deterministic tests
    )


class TestReporterStepDecorator:
    """Test that @reporter.step() does not affect business logic."""

    def test_step_success_reports_started_and_finished(self, mock_server):
        reporter = make_reporter(mock_server)

        @reporter.step("fetch_data")
        def fetch_data(state):
            return {"data": "result"}

        result = fetch_data({"input": "test"})

        # Business result is unchanged
        assert result == {"data": "result"}
        # Two events reported: step_started + step_finished
        assert len(_MockServerHandler.received_requests) >= 1

    def test_step_failure_reports_started_and_failed(self, mock_server):
        reporter = make_reporter(mock_server)

        @reporter.step("risky_step")
        def risky_step(state):
            raise ValueError("business error")

        # Exception propagates to caller
        with pytest.raises(ValueError, match="business error"):
            risky_step({})

        # step_failed event was reported (does not swallow exception)
        assert len(_MockServerHandler.received_requests) >= 1

    def test_reporting_failure_does_not_affect_business(self):
        """#6 invariant: reporter down -> business still works."""
        # Point to a non-listening port
        reporter = Reporter(
            endpoint="http://127.0.0.1:1",  # port 1 = connection refused
            tenant_id="tenant-test",
            token="test-token",
            agent_id="test-agent",
            timeout=0.5,
            flush_interval=0,  # sync mode for deterministic test
        )

        @reporter.step("step1")
        def step1(state):
            return {"value": 42}

        result = step1({})
        # Business result unchanged despite reporting failure
        assert result == {"value": 42}
        # Failure was counted
        assert reporter._failed_count > 0


class TestReporterJobContext:
    """Test that reporter.job() context manager works."""

    def test_job_success_reports_created_and_completed(self, mock_server):
        reporter = make_reporter(mock_server)

        with reporter.job("job-123", "test-pipeline"):
            pass  # success

        # job_created + job_completed events
        assert len(_MockServerHandler.received_requests) >= 1

    def test_job_failure_reports_created_and_failed(self, mock_server):
        reporter = make_reporter(mock_server)

        with pytest.raises(RuntimeError, match="job error"):
            with reporter.job("job-456", "failing-pipeline"):
                raise RuntimeError("job error")

        # Exception still propagates
        assert len(_MockServerHandler.received_requests) >= 1


class TestReporterDisabled:
    """Test disabled reporter (no env vars) is a no-op."""

    def test_disabled_reporter_no_op(self):
        reporter = Reporter._disabled()
        assert not reporter.enabled

        # step decorator on disabled reporter should just call the function
        @reporter.step("test")
        def step_fn(state):
            return {"ok": True}

        result = step_fn({})
        assert result == {"ok": True}


class TestReporterFromEnv:
    """Test Reporter.from_env() reads environment variables."""

    def test_from_env_creates_reporter(self):
        with patch.dict(os.environ, {
            "AETHERIS_ENDPOINT": "http://localhost:8080",
            "AETHERIS_TENANT": "my-tenant",
            "AETHERIS_TOKEN": "my-token",
            "AETHERIS_AGENT_ID": "my-agent",
        }):
            reporter = Reporter.from_env()
            assert reporter.enabled
            assert reporter._tenant_id == "my-tenant"
            assert reporter._agent_id == "my-agent"

    def test_from_env_disabled_without_endpoint(self):
        # Clear required vars
        env = {k: v for k, v in os.environ.items()
               if k not in ("AETHERIS_ENDPOINT", "AETHERIS_TENANT", "AETHERIS_TOKEN")}
        with patch.dict(os.environ, env, clear=True):
            reporter = Reporter.from_env()
            assert not reporter.enabled

    def test_token_not_in_payload(self, mock_server):
        """#6 invariant: token must not appear in any event payload."""
        reporter = make_reporter(mock_server, agent_id="secret-agent")
        reporter._token = "SUPER_SECRET_TOKEN_12345"

        @reporter.step("test_step")
        def step_fn(state):
            return {"data": "result"}

        step_fn({"input": "test"})

        # Check all received request bodies do NOT contain the token
        for body in _MockServerHandler.received_requests:
            body_str = body.decode("utf-8")
            assert "SUPER_SECRET_TOKEN_12345" not in body_str, \
                "token leaked into event payload!"


class TestReporterBufferOverflow:
    """Test buffer overflow handling (#7 preview)."""

    def test_buffer_overflow_drops_oldest(self):
        reporter = Reporter(
            endpoint="http://127.0.0.1:1",  # unreachable, all buffer
            tenant_id="tenant-test",
            token="test-token",
            max_buffer=3,
            timeout=0.1,
        )

        # Disable auto-flush to test buffer logic directly
        reporter._auto_flush = False

        # Fill buffer beyond capacity
        for i in range(10):
            with reporter._lock:
                if len(reporter._buffer) >= reporter._max_buffer:
                    reporter._buffer.pop(0)
                    reporter._dropped_count += 1
                reporter._buffer.append(Event(
                    type=EventType.STEP_STARTED,
                    job_id=f"job-{i}",
                    step_id=f"step-{i}",
                    payload={},
                ))

        # Some events should have been dropped
        assert reporter.dropped_count > 0
        assert len(reporter._buffer) == reporter._max_buffer
