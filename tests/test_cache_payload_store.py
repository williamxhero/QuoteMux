from __future__ import annotations

from datetime import datetime

from quotemux.store import payload_store
from quotemux.store.payload_store import delete_payload, get_payload, put_payload


def test_file_payload_store_roundtrip_and_deduplicates(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QUOTEMUX_CACHE_PAYLOAD_ROOT", str(tmp_path))

    first = put_payload("stocks.quotes.daily", datetime(2026, 4, 3), {"b": 2, "a": 1}, {"source": "unit"})
    second = put_payload("stocks.quotes.daily", datetime(2026, 4, 3), {"a": 1, "b": 2}, {"source": "unit"})

    assert first == second
    assert first.payload_path.endswith(".json.gz")
    assert "/2026-04/" in f"/{first.payload_path}"
    assert get_payload(first) == {"a": 1, "b": 2}
    assert len(tuple(tmp_path.rglob("*.json.gz"))) == 2


def test_file_payload_delete_uses_safe_del(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QUOTEMUX_CACHE_PAYLOAD_ROOT", str(tmp_path))
    deleted: list[list[str]] = []
    monkeypatch.setattr(payload_store.subprocess, "run", lambda args, check: deleted.append(args))

    payload_ref = put_payload("stocks.quotes.daily", datetime(2026, 4, 3), {"a": 1}, {"source": "unit"})
    delete_payload(payload_ref)

    assert len(deleted) == 2
    assert all(item[0] == "safe-del" for item in deleted)
