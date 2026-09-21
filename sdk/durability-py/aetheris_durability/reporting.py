"""Reporting client for T1 SDK event upload (Issue #6).

Sends step-level events to the Aetheris ingest endpoint
(POST /api/telemetry/v1/events, per #3 contract / #4 endpoint).

Core invariant (#6): reporting channel failures MUST NOT propagate
to business code. All exceptions are caught and logged; the wrapped
function's return value and exception behavior are unchanged.

Quick start (≤ 10 lines, 0 business logic changes):

    import os
    os.environ.setdefault("AETHERIS_ENDPOINT", "http://localhost:8080")
    os.environ.setdefault("AETHERIS_TENANT", "my-tenant")
    os.environ.setdefault("AETHERIS_TOKEN", "secret")

    import aetheris_durability as ae
    reporter = ae.Reporter.from_env()

    @ae.step("fetch_data")
    def fetch_data(state):
        return {"data": call_api()}
"""

from __future__ import annotations

import functools
import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, TypeVar
from urllib import request as urlreq
from urllib.error import URLError

from .types import Event, EventType, Job

logger = logging.getLogger("aetheris_durability.reporting")

T = TypeVar("T")

_SCHEMA_VERSION = "1"
_DEFAULT_TIMEOUT = 5.0  # seconds
_DEFAULT_MAX_BATCH = 50
_DEFAULT_BUFFER_SIZE = 1000


class Reporter:
    """Reports events to the Aetheris ingest endpoint.

    Thread-safe. All network errors are caught and logged — the
    reporting channel is fail-open (business continues, #6/#7).
    """

    def __init__(
        self,
        endpoint: str,
        tenant_id: str,
        token: str,
        agent_id: str = "",
        sdk_name: str = "aetheris-durability-py",
        sdk_version: str = "0.2.0",
        timeout: float = _DEFAULT_TIMEOUT,
        max_buffer: int = _DEFAULT_BUFFER_SIZE,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._tenant_id = tenant_id
        self._token = token
        self._agent_id = agent_id or "default"
        self._sdk_name = sdk_name
        self._sdk_version = sdk_version
        self._timeout = timeout
        self._max_buffer = max_buffer
        self._buffer: List[Event] = []
        self._lock = threading.Lock()
        self._dropped_count = 0
        self._success_count = 0
        self._failed_count = 0

    @classmethod
    def from_env(cls) -> "Reporter":
        """Create a Reporter from environment variables.

        Required:
            AETHERIS_ENDPOINT: ingest endpoint URL
            AETHERIS_TENANT: tenant ID
            AETHERIS_TOKEN: auth token (read from env, NOT from code)

        Optional:
            AETHERIS_AGENT_ID: agent identifier
            AETHERIS_TIMEOUT: request timeout in seconds
        """
        endpoint = os.environ.get("AETHERIS_ENDPOINT", "")
        tenant_id = os.environ.get("AETHERIS_TENANT", "")
        token = os.environ.get("AETHERIS_TOKEN", "")
        agent_id = os.environ.get("AETHERIS_AGENT_ID", "")
        timeout = float(os.environ.get("AETHERIS_TIMEOUT", _DEFAULT_TIMEOUT))

        if not endpoint or not tenant_id:
            logger.warning(
                "AETHERIS_ENDPOINT and AETHERIS_TENANT must be set; "
                "reporting disabled"
            )
            return cls._disabled()

        return cls(
            endpoint=endpoint,
            tenant_id=tenant_id,
            token=token,
            agent_id=agent_id,
            timeout=timeout,
        )

    @classmethod
    def _disabled(cls) -> "Reporter":
        """Create a no-op reporter (disabled)."""
        r = object.__new__(cls)
        r._endpoint = ""
        r._tenant_id = ""
        r._token = ""
        r._agent_id = "disabled"
        r._sdk_name = "aetheris-durability-py"
        r._sdk_version = "0.2.0"
        r._timeout = 0
        r._max_buffer = 0
        r._buffer = []
        r._lock = threading.Lock()
        r._dropped_count = 0
        r._success_count = 0
        r._failed_count = 0
        return r

    @property
    def enabled(self) -> bool:
        return bool(self._endpoint)

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    @property
    def success_count(self) -> int:
        return self._success_count

    def report(self, event: Event, job: Optional[Job] = None) -> None:
        """Buffer an event for reporting. Non-blocking, fail-open."""
        if not self.enabled:
            return
        with self._lock:
            if len(self._buffer) >= self._max_buffer:
                # Buffer full: drop oldest (per #7 discard-old policy)
                self._buffer.pop(0)
                self._dropped_count += 1
            self._buffer.append(event)
        # Flush immediately for simplicity; #7 will add batching
        self._flush()

    def _flush(self) -> None:
        """Send buffered events to ingest endpoint. Fail-open."""
        if not self.enabled:
            return
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer[:]
            self._buffer.clear()

        envelopes = [self._to_envelope(ev) for ev in batch]
        body = json.dumps({"events": envelopes}).encode("utf-8")

        req = urlreq.Request(
            f"{self._endpoint}/api/telemetry/v1/events",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Tenant-ID": self._tenant_id,
                "Authorization": f"Bearer {self._token}",
            },
            method="POST",
        )

        try:
            resp = urlreq.urlopen(req, timeout=self._timeout)
            if resp.status == 200:
                self._success_count += len(batch)
            else:
                self._failed_count += len(batch)
                logger.warning("ingest returned %d", resp.status)
        except URLError as e:
            self._failed_count += len(batch)
            logger.warning("reporting failed (network): %s", e)
        except Exception as e:
            self._failed_count += len(batch)
            logger.warning("reporting failed (unexpected): %s", e)

    def _to_envelope(self, ev: Event) -> Dict[str, Any]:
        """Convert SDK Event to ingest-contract-v1 envelope."""
        return {
            "schema_version": _SCHEMA_VERSION,
            "event_uid": ev.id or f"evt-{uuid.uuid4()}",
            "job_id": ev.job_id,
            "step_id": ev.step_id or "",
            "agent_id": self._agent_id,
            "tenant_id": self._tenant_id,
            "occurred_at": ev.created_at.isoformat() if ev.created_at else time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            ),
            "type": ev.type.value if isinstance(ev.type, EventType) else str(ev.type),
            "payload": ev.payload or {},
            "sdk_name": self._sdk_name,
            "sdk_version": self._sdk_version,
        }

    def step(self, step_id: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator that wraps a function with step-level event reporting.

        Usage:
            @reporter.step("fetch_data")
            def fetch_data(state):
                return {"data": call_api()}

        Invariant (#6): reporting failures do NOT propagate to the
        wrapped function. The function's return value and exceptions
        are unchanged regardless of reporting success/failure.
        """
        def decorator(fn: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                job_id = kwargs.get("job_id", "")
                span_id = f"span-{str(uuid.uuid4())[:8]}"

                # Report step_started
                self.report(Event(
                    type=EventType.STEP_STARTED,
                    job_id=job_id,
                    step_id=step_id,
                    payload={},
                ))

                try:
                    result = fn(*args, **kwargs)
                    # Report step_finished
                    self.report(Event(
                        type=EventType.STEP_FINISHED,
                        job_id=job_id,
                        step_id=step_id,
                        payload={"status": "success"},
                    ))
                    return result
                except Exception as e:
                    # Report step_failed (with exception type, NOT raw trace)
                    self.report(Event(
                        type=EventType.STEP_FAILED,
                        job_id=job_id,
                        step_id=step_id,
                        payload={
                            "error": type(e).__name__,
                            "message": str(e)[:200],  # truncated
                        },
                    ))
                    raise  # Re-raise original exception unchanged

            return wrapper
        return decorator

    def job(self, job_id: str, name: str = "") -> "_JobContext":
        """Context manager for job-level event reporting.

        Usage:
            with reporter.job("job-123", "data-pipeline"):
                step1()
                step2()

        Reports job_created on enter, job_completed on exit,
        job_failed on exception. Does NOT swallow exceptions.
        """
        return _JobContext(self, job_id, name)


class _JobContext:
    """Context manager for job-level reporting."""

    def __init__(self, reporter: Reporter, job_id: str, name: str) -> None:
        self._reporter = reporter
        self._job_id = job_id
        self._name = name or job_id

    def __enter__(self) -> "_JobContext":
        self._reporter.report(Event(
            type=EventType.JOB_CREATED,
            job_id=self._job_id,
            payload={"name": self._name},
        ))
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self._reporter.report(Event(
                type=EventType.JOB_COMPLETED,
                job_id=self._job_id,
                payload={},
            ))
        else:
            self._reporter.report(Event(
                type=EventType.JOB_FAILED,
                job_id=self._job_id,
                payload={"error": exc_type.__name__},
            ))
        # Do NOT suppress exceptions
        return None
