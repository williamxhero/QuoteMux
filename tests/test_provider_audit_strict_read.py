from __future__ import annotations

import json


def test_strict_public_read_skips_provider_audit_without_creating_or_modifying_file(monkeypatch, tmp_path) -> None:
    from quotemux.runtime_core import audit
    from quotemux.strict_read import strict_public_read_boundary

    monkeypatch.setattr(audit, "AUDIT_ROOT", tmp_path)
    path = tmp_path / "audit" / "date=20261030" / "events.jsonl"
    path.parent.mkdir(parents=True)
    original = b'{"existing": true}\n'
    path.write_bytes(original)

    with strict_public_read_boundary():
        audit.record_provider_event("stocks.quotes.daily", "provider_a", "success", {"request_count": 1})

    assert path.read_bytes() == original
    assert path.stat().st_size == len(original)


def test_non_strict_provider_audit_writes_one_readable_event(monkeypatch, tmp_path) -> None:
    from quotemux.runtime_core import audit

    monkeypatch.setattr(audit, "AUDIT_ROOT", tmp_path)
    audit.record_provider_event("stocks.quotes.daily", "provider_a", "success", {"request_count": 1})

    files = list(tmp_path.glob("audit/date=*/events.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["contract_name"] == "stocks.quotes.daily"
    assert event["provider"] == "provider_a"
    assert event["status"] == "success"
    assert event["detail"] == {"request_count": 1}
