from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


PAYLOAD_ROOT = Path(os.getenv("QUOTEMUX_CACHE_PAYLOAD_ROOT", "/volume/stocks/QuoteMux/cache_payloads"))


@dataclass(frozen=True)
class CachePayloadRef:
    payload_sha256: str
    payload_path: str
    source_sha256: str
    source_path: str


def _canonical_json_bytes(payload: dict[str, object]) -> bytes:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text.encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _capability_dir(capability_id: str) -> str:
    return _sha256(capability_id.encode("utf-8"))[:16]


def _month_dir(time_key: datetime) -> str:
    return time_key.strftime("%Y-%m")


def _relative_path(capability_id: str, time_key: datetime, sha256: str, kind: str) -> str:
    return str(Path(_capability_dir(capability_id)) / _month_dir(time_key) / f"{kind}_{sha256}.json.gz").replace("\\", "/")


def _safe_delete_path(path: Path) -> None:
    if not path.exists():
        return
    subprocess.run(["safe-del", str(path)], check=True)


def _write_compressed_json(relative_path: str, data: bytes) -> None:
    target = PAYLOAD_ROOT / relative_path
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=target.name, suffix=".tmp", dir=target.parent, delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            with gzip.GzipFile(fileobj=temp_file, mode="wb", mtime=0) as gzip_file:
                gzip_file.write(data)
        if not target.exists():
            os.replace(temp_path, target)
        else:
            _safe_delete_path(temp_path)
    except Exception:
        if temp_path is not None:
            _safe_delete_path(temp_path)
        raise


def put_payload(capability_id: str, time_key: datetime, payload_json: dict[str, object], source_json: dict[str, object]) -> CachePayloadRef:
    payload_data = _canonical_json_bytes(payload_json)
    source_data = _canonical_json_bytes(source_json)
    payload_sha256 = _sha256(payload_data)
    source_sha256 = _sha256(source_data)
    payload_path = _relative_path(capability_id, time_key, payload_sha256, "payload")
    source_path = _relative_path(capability_id, time_key, source_sha256, "source")
    _write_compressed_json(payload_path, payload_data)
    _write_compressed_json(source_path, source_data)
    return CachePayloadRef(payload_sha256, payload_path, source_sha256, source_path)


def _read_payload_file(path: str, expected_sha256: str) -> dict[str, object] | None:
    full_path = PAYLOAD_ROOT / path
    try:
        with gzip.open(full_path, "rb") as payload_file:
            data = payload_file.read()
    except Exception:
        return None
    if _sha256(data) != expected_sha256:
        return None
    payload = json.loads(data.decode("utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def get_payload(payload_ref: CachePayloadRef) -> dict[str, object] | None:
    return _read_payload_file(payload_ref.payload_path, payload_ref.payload_sha256)


def delete_payload(payload_ref: CachePayloadRef) -> None:
    _safe_delete_path(PAYLOAD_ROOT / payload_ref.payload_path)
    _safe_delete_path(PAYLOAD_ROOT / payload_ref.source_path)
