from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
import random
import threading
import time
from typing import Callable, TypeVar


T = TypeVar("T")


def _int_env(name: str, default: int) -> int:
    text = os.getenv(name, "")
    try:
        return int(text)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    text = os.getenv(name, "")
    try:
        return float(text)
    except ValueError:
        return default


GLOBAL_CONCURRENCY = _int_env("MHK_GLOBAL_CONCURRENCY", 16)
PROVIDER_MAX_RETRIES = _int_env("MHK_PROVIDER_MAX_RETRIES", 2)
PROVIDER_BACKOFF_SECONDS = _float_env("MHK_PROVIDER_BACKOFF_SECONDS", 0.2)
PROVIDER_QUEUE_TIMEOUT_SECONDS = _float_env("MHK_PROVIDER_QUEUE_TIMEOUT_SECONDS", 10.0)


@dataclass(frozen=True)
class ProviderPolicy:
    concurrency: int
    calls_per_second: float
    max_retries: int
    queue_timeout_seconds: float


PROVIDER_POLICIES = {
    "tushare": ProviderPolicy(
        _int_env("MHK_TUSHARE_CONCURRENCY", 4),
        _float_env("MHK_TUSHARE_RPS", 3.0),
        _int_env("MHK_TUSHARE_MAX_RETRIES", PROVIDER_MAX_RETRIES),
        _float_env("MHK_TUSHARE_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
    "opentdx": ProviderPolicy(
        _int_env("MHK_OPENTDX_CONCURRENCY", 4),
        _float_env("MHK_OPENTDX_RPS", 4.0),
        _int_env("MHK_OPENTDX_MAX_RETRIES", PROVIDER_MAX_RETRIES),
        _float_env("MHK_OPENTDX_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
    "efinance": ProviderPolicy(
        _int_env("MHK_EFINANCE_CONCURRENCY", 4),
        _float_env("MHK_EFINANCE_RPS", 4.0),
        _int_env("MHK_EFINANCE_MAX_RETRIES", PROVIDER_MAX_RETRIES),
        _float_env("MHK_EFINANCE_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
    "mootdx": ProviderPolicy(
        _int_env("MHK_MOOTDX_CONCURRENCY", 3),
        _float_env("MHK_MOOTDX_RPS", 3.0),
        _int_env("MHK_MOOTDX_MAX_RETRIES", PROVIDER_MAX_RETRIES),
        _float_env("MHK_MOOTDX_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
    "akshare": ProviderPolicy(
        _int_env("MHK_AKSHARE_CONCURRENCY", 3),
        _float_env("MHK_AKSHARE_RPS", 2.0),
        _int_env("MHK_AKSHARE_MAX_RETRIES", PROVIDER_MAX_RETRIES),
        _float_env("MHK_AKSHARE_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
    "store_db": ProviderPolicy(
        _int_env("MHK_STORE_DB_CONCURRENCY", 8),
        _float_env("MHK_STORE_DB_RPS", 0.0),
        _int_env("MHK_STORE_DB_MAX_RETRIES", 0),
        _float_env("MHK_STORE_DB_QUEUE_TIMEOUT_SECONDS", PROVIDER_QUEUE_TIMEOUT_SECONDS),
    ),
}


class ProviderGate:
    def __init__(self, provider: str, policy: ProviderPolicy):
        self.provider = provider
        self.policy = policy
        self._semaphore = threading.BoundedSemaphore(max(1, policy.concurrency))
        self._lock = threading.Lock()
        self._calls: deque[float] = deque()
        self._active = 0
        self._queued = 0
        self._total_requests = 0
        self._total_errors = 0
        self._total_retries = 0
        self._rate_waits = 0
        self._queue_wait_seconds = 0.0
        self._runtime_seconds = 0.0
        self._api_metrics: dict[str, dict[str, int]] = {}

    def _api_metric(self, api_name: str) -> dict[str, int]:
        metric = self._api_metrics.get(api_name)
        if metric is None:
            metric = {"active": 0, "queued": 0, "requests": 0, "errors": 0, "retries": 0}
            self._api_metrics[api_name] = metric
        return metric

    def run(self, api_name: str, func: Callable[..., T], *args: object, **kwargs: object) -> T:
        attempt = 0
        while True:
            try:
                return self._run_once(api_name, func, *args, **kwargs)
            except TimeoutError:
                with self._lock:
                    self._total_errors += 1
                    self._api_metric(api_name)["errors"] += 1
                raise
            except Exception:
                if attempt >= self.policy.max_retries:
                    with self._lock:
                        self._total_errors += 1
                        self._api_metric(api_name)["errors"] += 1
                    raise
                attempt += 1
                with self._lock:
                    self._total_retries += 1
                    self._api_metric(api_name)["retries"] += 1
                delay = PROVIDER_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0.0, PROVIDER_BACKOFF_SECONDS)
                time.sleep(delay)

    def _run_once(self, api_name: str, func: Callable[..., T], *args: object, **kwargs: object) -> T:
        queue_start = time.monotonic()
        with self._lock:
            self._queued += 1
            self._api_metric(api_name)["queued"] += 1
        acquired = _acquire_semaphore(self._semaphore, self.policy.queue_timeout_seconds)
        queue_seconds = time.monotonic() - queue_start
        if not acquired:
            with self._lock:
                self._queued -= 1
                self._api_metric(api_name)["queued"] -= 1
                self._queue_wait_seconds += queue_seconds
            raise TimeoutError(f"{self.provider}.{api_name} provider queue timeout")
        with self._lock:
            self._queued -= 1
            self._active += 1
            metric = self._api_metric(api_name)
            metric["queued"] -= 1
            metric["active"] += 1
            self._queue_wait_seconds += queue_seconds
        try:
            self._wait_for_rate_slot()
            global_queue_start = time.monotonic()
            global_acquired = _acquire_semaphore(_GLOBAL_GATE, self.policy.queue_timeout_seconds)
            global_queue_seconds = time.monotonic() - global_queue_start
            with self._lock:
                self._queue_wait_seconds += global_queue_seconds
            if not global_acquired:
                raise TimeoutError(f"{self.provider}.{api_name} global provider queue timeout")
            start = time.monotonic()
            with self._lock:
                self._total_requests += 1
                self._api_metric(api_name)["requests"] += 1
            try:
                result = func(*args, **kwargs)
                with self._lock:
                    self._runtime_seconds += time.monotonic() - start
                return result
            finally:
                _GLOBAL_GATE.release()
        finally:
            with self._lock:
                self._active -= 1
                self._api_metric(api_name)["active"] -= 1
            self._semaphore.release()

    def _wait_for_rate_slot(self) -> None:
        if self.policy.calls_per_second <= 0:
            return
        window = 1.0
        while True:
            wait_seconds = 0.0
            with self._lock:
                now = time.monotonic()
                cutoff = now - window
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self.policy.calls_per_second:
                    self._calls.append(now)
                    return
                wait_seconds = window - (now - self._calls[0]) + 0.001
                self._rate_waits += 1
            time.sleep(max(0.001, wait_seconds))

    def snapshot(self) -> dict[str, int | float | str]:
        with self._lock:
            total_requests = self._total_requests
            total_errors = self._total_errors
            api_metrics = {api_name: metric.copy() for api_name, metric in sorted(self._api_metrics.items())}
            return {
                "provider": self.provider,
                "concurrency_limit": self.policy.concurrency,
                "calls_per_second_limit": self.policy.calls_per_second,
                "max_retries": self.policy.max_retries,
                "queue_timeout_seconds": self.policy.queue_timeout_seconds,
                "active": self._active,
                "queued": self._queued,
                "total_requests": total_requests,
                "total_errors": total_errors,
                "total_retries": self._total_retries,
                "rate_waits": self._rate_waits,
                "error_rate": round(total_errors / total_requests, 6) if total_requests else 0.0,
                "queue_wait_seconds": round(self._queue_wait_seconds, 6),
                "runtime_seconds": round(self._runtime_seconds, 6),
                "apis": api_metrics,
            }


_GLOBAL_GATE = threading.BoundedSemaphore(max(1, GLOBAL_CONCURRENCY))
_GATES = {provider: ProviderGate(provider, policy) for provider, policy in PROVIDER_POLICIES.items()}


def _acquire_semaphore(semaphore: threading.BoundedSemaphore, timeout_seconds: float) -> bool:
    if timeout_seconds <= 0:
        return semaphore.acquire(blocking=False)
    return semaphore.acquire(timeout=timeout_seconds)


def call_provider_api(provider: str, api_name: str, func: Callable[..., T], *args: object, **kwargs: object) -> T:
    gate = _GATES.get(provider)
    if gate is None:
        return func(*args, **kwargs)
    return gate.run(api_name, func, *args, **kwargs)


def get_provider_metrics() -> dict[str, object]:
    return {
        "global_concurrency_limit": GLOBAL_CONCURRENCY,
        "max_retries": PROVIDER_MAX_RETRIES,
        "backoff_seconds": PROVIDER_BACKOFF_SECONDS,
        "queue_timeout_seconds": PROVIDER_QUEUE_TIMEOUT_SECONDS,
        "providers": {provider: gate.snapshot() for provider, gate in _GATES.items()},
    }
