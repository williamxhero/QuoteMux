from __future__ import annotations

from collections import deque
from functools import lru_cache
import re
import threading
import time

from quotemux.infra.config import DOCS_ROOT
from quotemux.infra.provider_runtime.core import call_provider_api


TS_DOCS_ROOT = DOCS_ROOT / "3rdparty" / "ts"
API_NAME_RE = re.compile(r"接口[:：]\s*([A-Za-z0-9_]+)")
PER_MINUTE_RE = re.compile(r"每分钟[^0-9]{0,12}(\d+)\s*次")


class RateLimiter:
    def __init__(self, max_calls_per_minute: int):
        self.max_calls_per_minute = max_calls_per_minute
        self._lock = threading.Lock()
        self._calls: deque[float] = deque()

    def call(self, func, *args, **kwargs):
        if self.max_calls_per_minute <= 0:
            return func(*args, **kwargs)
        while True:
            wait_seconds = 0.0
            with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls_per_minute:
                    self._calls.append(now)
                    break
                wait_seconds = 60.0 - (now - self._calls[0]) + 0.01
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        return func(*args, **kwargs)


@lru_cache(maxsize=1)
def build_api_rate_limit_map() -> dict[str, int]:
    limit_map: dict[str, int] = {}
    if not TS_DOCS_ROOT.exists():
        return limit_map
    for path in TS_DOCS_ROOT.glob("*.md"):
        text = path.read_text(encoding="utf-8").lstrip("\ufeff")
        api_name_match = API_NAME_RE.search(text)
        if api_name_match is None:
            continue
        api_name = api_name_match.group(1)
        minute_limits = [int(value) for value in PER_MINUTE_RE.findall(text)]
        if minute_limits == []:
            continue
        limit_map[api_name] = min(minute_limits)
    return limit_map


def get_api_rate_limit(api_name: str) -> int | None:
    return build_api_rate_limit_map().get(api_name)


@lru_cache(maxsize=128)
def get_api_rate_limiter(api_name: str) -> RateLimiter | None:
    limit = get_api_rate_limit(api_name)
    if limit is None:
        return None
    return RateLimiter(max_calls_per_minute=limit)


def call_tushare_api(api_name: str, func, *args, **kwargs):
    limiter = get_api_rate_limiter(api_name)
    if limiter is None:
        return call_provider_api("tushare", api_name, func, *args, **kwargs)
    return call_provider_api("tushare", api_name, limiter.call, func, *args, **kwargs)

