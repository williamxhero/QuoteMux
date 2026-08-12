from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ProviderContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


T = TypeVar("T", bound=BaseModel)


class AuditedRecord(ProviderContractModel, Generic[T]):
    source_event_id: str = Field(min_length=1)
    raw_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_projection: dict[str, object]
    data: T


class AuditedPage(ProviderContractModel, Generic[T]):
    capability_id: str
    data_version: str
    provider: str = Field(min_length=1)
    source: str
    source_version: str
    fetched_at: str
    confirmed_empty: bool
    next_cursor: str
    records: list[AuditedRecord[T]]

    @field_validator("fetched_at")
    @classmethod
    def _validate_fetched_at(cls, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("fetched_at 必须是 RFC3339 时间") from exc
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise ValueError("fetched_at 必须使用 UTC")
        return value

    @model_validator(mode="after")
    def _validate_empty_contract(self) -> AuditedPage[T]:
        if self.confirmed_empty and (self.records != [] or self.next_cursor != ""):
            raise ValueError("confirmed_empty 只能对应空 records 且无 next_cursor")
        if not self.confirmed_empty and self.records == [] and self.next_cursor == "":
            raise ValueError("终止空页必须标记 confirmed_empty")
        return self


def canonical_json_sha256(raw_projection: dict[str, object]) -> str:
    _validate_canonical_json_value(raw_projection)
    payload = json.dumps(
        raw_projection,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def _validate_canonical_json_value(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("canonical raw_projection 禁止 float")
    if value is None or isinstance(value, (str, int, bool)):
        return
    if isinstance(value, list):
        for item in value:
            _validate_canonical_json_value(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical raw_projection 的 key 必须是字符串")
            _validate_canonical_json_value(item)
        return
    raise TypeError(f"canonical raw_projection 不支持类型: {type(value).__name__}")
