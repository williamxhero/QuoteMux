from __future__ import annotations

from collections.abc import Callable, Sized
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import replace
from datetime import datetime
import math
import time
from typing import TypeVar

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.provider_timeout.adaptive import resolve_provider_timeout
from quotemux.provider_timeout.metrics import record_provider_timeout_metric
from quotemux.provider_timeout.policy import ProviderTimeoutMetric, TIMEOUT_STATUS_EMPTY, TIMEOUT_STATUS_ERROR, TIMEOUT_STATUS_SUCCESS, TIMEOUT_STATUS_TIMEOUT
from quotemux.source_packages.instance_context import use_source_instance


T = TypeVar("T")


def run_provider_request(
    capability_id: str,
    provider: str,
    source_instance_id: str,
    handler: str,
    source_instance: SourceInstanceConfig | None,
    fetcher: Callable[[], list[T]],
    capability_remaining_seconds: float | None,
) -> list[T]:
    source_timeout = None if source_instance is None else source_instance.timeout_seconds
    resolved = resolve_provider_timeout(capability_id, provider, source_timeout)
    timeout_seconds = _merge_timeout(resolved.timeout_seconds, capability_remaining_seconds)
    started_at = time.perf_counter()
    status = TIMEOUT_STATUS_ERROR
    row_count = 0
    error_text = ""
    try:
        if timeout_seconds <= 0:
            raise TimeoutError(f"{provider}.{handler} provider timeout")
        result = _run_with_timeout(
            _provider_call(source_instance, timeout_seconds, fetcher),
            timeout_seconds,
        )
        row_count = _row_count(result)
        status = TIMEOUT_STATUS_EMPTY if row_count == 0 else TIMEOUT_STATUS_SUCCESS
        return result
    except FutureTimeoutError as exc:
        status = TIMEOUT_STATUS_TIMEOUT
        error_text = f"{provider}.{handler} provider timeout"
        raise TimeoutError(error_text) from exc
    except TimeoutError as exc:
        status = TIMEOUT_STATUS_TIMEOUT
        error_text = str(exc)
        raise
    except Exception as exc:
        status = TIMEOUT_STATUS_ERROR
        error_text = str(exc)
        raise
    finally:
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 3)
        record_provider_timeout_metric(
            ProviderTimeoutMetric(
                capability_id=capability_id,
                provider=provider,
                source_instance_id=source_instance_id,
                handler=handler,
                status=status,
                elapsed_ms=elapsed_ms,
                effective_timeout_seconds=round(timeout_seconds, 3),
                row_count=row_count,
                error_text=error_text,
                created_at=datetime.now(),
            )
        )


def _provider_call(source_instance: SourceInstanceConfig | None, timeout_seconds: float, fetcher: Callable[[], list[T]]) -> Callable[[], list[T]]:
    if source_instance is None:
        return fetcher
    actual_instance = source_instance
    if source_instance.timeout_seconds is None:
        actual_instance = replace(source_instance, timeout_seconds=max(1, math.ceil(timeout_seconds)))

    def call() -> list[T]:
        with use_source_instance(actual_instance):
            return fetcher()

    return call


def _run_with_timeout(func: Callable[[], list[T]], timeout_seconds: float) -> list[T]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(func)
    try:
        return future.result(timeout=timeout_seconds)
    except Exception:
        future.cancel()
        raise
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _merge_timeout(provider_timeout_seconds: float, capability_remaining_seconds: float | None) -> float:
    if capability_remaining_seconds is None:
        return provider_timeout_seconds
    return min(provider_timeout_seconds, max(0.0, capability_remaining_seconds))


def _row_count(value: object) -> int:
    if isinstance(value, Sized) and not isinstance(value, (str, bytes, bytearray)):
        return len(value)
    return 1
