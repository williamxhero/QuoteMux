from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def prevent_implicit_provider_environment_install(monkeypatch):
    """Keep the canonical unit suite offline when a fallback reaches an isolated provider."""

    attempted_packages: list[str] = []

    def reject_install(manifest):
        attempted_packages.append(manifest.package_id)
        raise AssertionError(f"unexpected provider environment install: {manifest.package_id}")

    monkeypatch.setattr("quotemux.source_packages.isolated.ensure_package_environment", reject_install)
    yield
    assert attempted_packages == []
