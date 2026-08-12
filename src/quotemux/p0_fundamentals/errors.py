from __future__ import annotations


P0_ERROR_KINDS = {
    "request_error",
    "timeout_error",
    "rate_limit_error",
    "permission_error",
    "parse_error",
    "schema_error",
    "contract_error",
    "cache_error",
    "database_error",
}


class P0QueryError(RuntimeError):
    def __init__(self, kind: str, message: str) -> None:
        if kind not in P0_ERROR_KINDS:
            raise ValueError(f"未知 P0 错误类型: {kind}")
        super().__init__(message)
        self.kind = kind


class P0CacheWriteError(RuntimeError):
    pass
