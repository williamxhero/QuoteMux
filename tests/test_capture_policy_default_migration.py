from __future__ import annotations

from datetime import time

from quotemux.store import capture
from quotemux.store.capture import CapturePolicy


def _money_flow_policy(*, enabled: bool = True) -> CapturePolicy:
    return CapturePolicy(
        capability_id="stocks.indicators.money_flow",
        enabled=enabled,
        cadence="daily",
        run_time=time(18, 0),
        timezone="Asia/Shanghai",
        weekday=None,
        month=None,
        month_day=None,
        scope_profile="active_stocks_recent_trading_days",
        window_count=30,
        batch_size=100,
        notes="",
    )


def test_new_install_enables_only_batch_money_flow_capture() -> None:
    specs = {spec.capability_id: spec for spec in capture.DEFAULT_CAPTURE_POLICY_SPECS}

    assert specs["stocks.indicators.money_flow"].enabled is False
    assert specs["stocks.indicators.money_flow.batch"].enabled is True


def test_schema_upgrade_migrates_only_exact_legacy_defaults(monkeypatch) -> None:
    many_calls: list[tuple[str, list[tuple[object, ...]]]] = []
    sql_calls: list[str] = []
    monkeypatch.setattr(capture, "_CAPTURE_SCHEMA_READY", False)
    monkeypatch.setattr(capture, "_CAPTURE_SCHEMA_FAILED", False)
    monkeypatch.setattr(capture, "_ensure_schema", lambda: True)
    monkeypatch.setattr(capture, "execute_sql", lambda query, params=(): sql_calls.append(query) is None or True)
    monkeypatch.setattr(capture, "execute_many", lambda query, params: many_calls.append((query, params)) is None or True)

    assert capture._ensure_capture_schema() is True

    legacy_query, legacy_params = next(call for call in many_calls if "managed_by_default is null" in call[0])
    seed_query, seed_params = next(call for call in many_calls if "insert into capability_capture_policy" in call[0])
    legacy_by_id = {str(params[0]): params for params in legacy_params}
    seed_by_id = {str(params[0]): params for params in seed_params}

    assert legacy_by_id["stocks.indicators.money_flow"][1] is True
    assert seed_by_id["stocks.indicators.money_flow"][1] is False
    assert seed_by_id["stocks.indicators.money_flow.batch"][1] is True
    assert "notes = ''" in legacy_query
    assert "run_time = %s" in legacy_query
    assert "when capability_capture_policy.managed_by_default" in seed_query
    assert "else capability_capture_policy.scope_profile" in seed_query
    assert "enabled =" not in seed_query.split("on conflict", maxsplit=1)[1]
    assert any("set managed_by_default = false" in query for query in sql_calls)

    conditional_disable = next(query for query in sql_calls if "update capability_capture_policy single_policy" in query)
    assert "single_policy.managed_by_default" in conditional_disable
    assert "batch_policy.enabled" in conditional_disable


def test_user_update_marks_policy_explicit(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(capture, "_ensure_capture_schema", lambda: True)

    def fake_execute(query: str, params: tuple[object, ...] = ()) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr(capture, "execute_sql", fake_execute)

    assert capture.CapturePolicyRepository().update(_money_flow_policy(enabled=True)) is True
    assert "managed_by_default = false" in str(captured["query"])
