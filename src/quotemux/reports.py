from __future__ import annotations

from dataclasses import dataclass, field

from quotemux.runtime_core.executor import FallbackReport


@dataclass(frozen=True)
class SourceInstanceReport:
    package_id: str
    source_instance_id: str
    handler: str
    request_count: int
    success_count: int
    error_count: int
    elapsed_ms: float

    def to_dict(self) -> dict[str, object]:
        return {
            "package_id": self.package_id,
            "source_instance_id": self.source_instance_id,
            "handler": self.handler,
            "request_count": self.request_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass(frozen=True)
class ContractReport:
    contract_name: str
    profile_id: str = ""
    profile_version: str = ""
    source_hit_counts: dict[str, int] = field(default_factory=dict)
    source_request_counts: dict[str, int] = field(default_factory=dict)
    source_instance_reports: tuple[SourceInstanceReport, ...] = ()
    source_error_count: int = 0
    source_skipped_count: int = 0
    conflict_count: int = 0
    quarantine_count: int = 0
    degraded: bool = False

    def package_reports(self) -> tuple[dict[str, object], ...]:
        packages: dict[str, dict[str, object]] = {}
        for item in self.source_instance_reports:
            package = packages.get(item.package_id)
            if package is None:
                package = {
                    "package_id": item.package_id,
                    "request_count": 0,
                    "success_count": 0,
                    "error_count": 0,
                    "elapsed_ms": 0.0,
                }
                packages[item.package_id] = package
            package["request_count"] = int(package["request_count"]) + item.request_count
            package["success_count"] = int(package["success_count"]) + item.success_count
            package["error_count"] = int(package["error_count"]) + item.error_count
            package["elapsed_ms"] = round(float(package["elapsed_ms"]) + item.elapsed_ms, 3)
        return tuple(packages.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_name": self.contract_name,
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "source_hit_counts": dict(self.source_hit_counts),
            "source_request_counts": dict(self.source_request_counts),
            "package_reports": list(self.package_reports()),
            "source_instance_reports": [item.to_dict() for item in self.source_instance_reports],
            "source_error_count": self.source_error_count,
            "source_skipped_count": self.source_skipped_count,
            "conflict_count": self.conflict_count,
            "quarantine_count": self.quarantine_count,
            "degraded": self.degraded,
        }

    @classmethod
    def empty(cls, contract_name: str, base_source_name: str = "", base_hit: bool = False) -> ContractReport:
        source_hit_counts = {base_source_name: int(base_hit)} if base_source_name else {}
        source_request_counts = {base_source_name: 1} if base_source_name else {}
        return cls(
            contract_name=contract_name,
            source_hit_counts=source_hit_counts,
            source_request_counts=source_request_counts,
        )

    @classmethod
    def from_fallback_report(
        cls,
        contract_name: str,
        fallback_report: FallbackReport,
        base_source_name: str = "",
        base_hit: bool = False,
        degraded: bool = False,
    ) -> ContractReport:
        source_hit_counts = fallback_report.provider_hit_counts()
        source_request_counts = fallback_report.provider_request_counts()
        if base_source_name:
            source_hit_counts = {base_source_name: int(base_hit), **source_hit_counts}
            source_request_counts = {base_source_name: 1, **source_request_counts}
        source_instance_reports = tuple(
            SourceInstanceReport(
                package_id=step.package_id,
                source_instance_id=step.source_instance_id,
                handler=step.handler,
                request_count=step.request_count,
                success_count=step.request_count - step.error_count,
                error_count=step.error_count,
                elapsed_ms=step.elapsed_ms,
            )
            for step in fallback_report.steps
        )
        return cls(
            contract_name=contract_name,
            profile_id=fallback_report.profile_id,
            profile_version=fallback_report.profile_version,
            source_hit_counts=source_hit_counts,
            source_request_counts=source_request_counts,
            source_instance_reports=source_instance_reports,
            source_error_count=fallback_report.total_error_count(),
            source_skipped_count=fallback_report.total_skipped_count(),
            conflict_count=fallback_report.total_conflict_count(),
            degraded=degraded,
        )

