from __future__ import annotations

from dataclasses import dataclass, field

from quotemux.runtime_core.executor import FallbackReport


@dataclass(frozen=True)
class ContractReport:
    contract_name: str
    source_hit_counts: dict[str, int] = field(default_factory=dict)
    source_request_counts: dict[str, int] = field(default_factory=dict)
    source_error_count: int = 0
    source_skipped_count: int = 0
    conflict_count: int = 0
    quarantine_count: int = 0
    degraded: bool = False

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
        return cls(
            contract_name=contract_name,
            source_hit_counts=source_hit_counts,
            source_request_counts=source_request_counts,
            source_error_count=fallback_report.total_error_count(),
            source_skipped_count=fallback_report.total_skipped_count(),
            conflict_count=fallback_report.total_conflict_count(),
            degraded=degraded,
        )

