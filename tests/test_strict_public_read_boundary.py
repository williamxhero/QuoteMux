from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest


def test_strict_public_read_boundary_is_nested_and_restores_state() -> None:
    from quotemux.strict_read import is_strict_public_read, strict_public_read_boundary

    assert is_strict_public_read() is False
    with strict_public_read_boundary():
        assert is_strict_public_read() is True
        with strict_public_read_boundary():
            assert is_strict_public_read() is True
        assert is_strict_public_read() is True
    assert is_strict_public_read() is False


def test_strict_public_read_context_can_be_bound_to_worker_thread() -> None:
    from quotemux.strict_read import bind_strict_public_read_context, is_strict_public_read, strict_public_read_boundary

    with ThreadPoolExecutor(max_workers=1) as executor:
        with strict_public_read_boundary():
            bound = bind_strict_public_read_context(is_strict_public_read)
            assert executor.submit(is_strict_public_read).result() is False
            assert executor.submit(bound).result() is True
            assert is_strict_public_read() is True
    assert is_strict_public_read() is False


def test_provider_choke_points_fail_before_handler_execution() -> None:
    from quotemux.infra.provider_runtime import core
    from quotemux.source_packages.registry import SourcePackageRegistry
    from quotemux.strict_read import StrictReadViolation, strict_public_read_boundary

    called: list[str] = []
    with strict_public_read_boundary():
        with pytest.raises(StrictReadViolation, match="provider"):
            core.call_provider_api("tushare", "daily", lambda: called.append("called"))
        with pytest.raises(StrictReadViolation, match="source_package"):
            SourcePackageRegistry(()).get_handler("missing", "daily")
    assert called == []


def test_provider_dependency_install_choke_points_fail_before_side_effects(monkeypatch) -> None:
    from quotemux import package_install
    from quotemux.source_packages import environment
    from quotemux.strict_read import StrictReadViolation, strict_public_read_boundary

    monkeypatch.setattr(package_install, "_install_distribution", lambda *_args: pytest.fail("installer must not run"))
    with strict_public_read_boundary():
        with pytest.raises(StrictReadViolation, match="package_install"):
            package_install.install_all_packages()
        with pytest.raises(StrictReadViolation, match="package_environment"):
            environment.ensure_package_environment(object())


def test_fact_cache_and_sql_write_choke_points_reject_inside_boundary(monkeypatch, tmp_path) -> None:
    from quotemux import fact_ref_writes
    from quotemux.infra.cache import store as file_cache
    from quotemux.infra.db import client as fact_db
    from quotemux.reports import ContractReport
    from quotemux.store import cache_db, runtime as store_runtime
    from quotemux.strict_read import StrictReadViolation, strict_public_read_boundary

    writer = fact_ref_writes.get_fact_ref_writer("stocks.quotes.daily")
    assert writer is not None
    monkeypatch.setattr(store_runtime, "get_postgres_cache_store", lambda: pytest.fail("cache store must not run"))
    monkeypatch.setattr(fact_db, "_db_available_for_attempt", lambda: pytest.fail("fact SQL must not run"))
    monkeypatch.setattr(cache_db, "_cache_db_available_for_attempt", lambda: pytest.fail("cache SQL must not run"))
    frame = pd.DataFrame([{"code": "600000"}])

    with strict_public_read_boundary():
        with pytest.raises(StrictReadViolation, match="fact_write"):
            writer([])
        with pytest.raises(StrictReadViolation, match="cache_write"):
            store_runtime.store_result("stocks.quotes.daily", {}, [], ContractReport("stocks.quotes.daily"))
        with pytest.raises(StrictReadViolation, match="sql_write"):
            fact_db.execute_sql("delete from fact.stock_daily_1d")
        with pytest.raises(StrictReadViolation, match="cache_sql_write"):
            cache_db.execute_sql("delete from capability_cache_rows")
        with pytest.raises(StrictReadViolation, match="file_cache_write"):
            file_cache.write_cache_frame(tmp_path / "data.parquet", frame)
    assert not (tmp_path / "data.parquet").exists()


def test_admin_capture_and_repair_remain_available_outside_boundary() -> None:
    from quotemux.infra.provider_runtime.core import call_provider_api
    from quotemux.store.admin import QuoteMuxCaptureAdmin

    class Job:
        def run_capture(self, capability_id: str):
            return {"capability_id": capability_id, "status": "success"}

        def run_repair(self, dataset: str, scope: dict[str, object], dataset_version: str = ""):
            return {"dataset": dataset, "scope": scope, "dataset_version": dataset_version, "status": "success"}

    admin = QuoteMuxCaptureAdmin(job=Job())
    assert call_provider_api("unknown_provider", "ping", lambda: "ok") == "ok"
    assert admin.run_capture("stocks.quotes.intraday")["status"] == "success"
    assert admin.run_repair("stocks.quotes.intraday", {"codes": ["600000"]}, "v2")["dataset_version"] == "v2"
