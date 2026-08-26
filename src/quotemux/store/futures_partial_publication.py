"""Immutable metadata-only partial publication identities for S000012."""
from __future__ import annotations
import hashlib
import json
from collections.abc import Mapping

DATASET_ID = "future_1m_partial_s000012_quotemux"

def canonical_identity(prefix: str, payload: Mapping[str, object]) -> str:
    if prefix not in {"qmp", "qmc", "qmg"}:
        raise ValueError("unsupported publication prefix")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return f"{prefix}-v1-" + hashlib.sha256(encoded).hexdigest()

def validate_identity(value: str, prefix: str) -> str:
    expected = f"{prefix}-v1-"
    if not value.startswith(expected) or len(value) != len(expected) + 64:
        raise ValueError(f"invalid {prefix} identity")
    int(value[len(expected):], 16)
    return value
