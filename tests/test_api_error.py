from __future__ import annotations

from platform_models import ApiError, ApiErrorDetails


def test_api_error_details_preserve_legacy_text_and_accept_bounded_repair_object() -> None:
    assert ApiError(code="BAD_REQUEST", message="bad", details="legacy").model_dump()["details"] == "legacy"

    error = ApiError(
        code="DATA_INCOMPLETE",
        message="local catalog missing",
        details={
            "dataset_id": "future_contract_reference",
            "reason": "no_complete_published_snapshot",
            "requested_codes": ["rb"],
            "include_expired": False,
            "repair_endpoint": "/api/admin/data-repairs",
            "repair_template": {"dataset_id": "future_contract_reference", "scope": {"codes": [], "include_expired": False}},
        },
    )

    assert isinstance(error.details, ApiErrorDetails)
    assert error.model_dump()["details"]["repair_template"]["scope"]["include_expired"] is False


def test_api_error_openapi_schema_declares_string_or_structured_details() -> None:
    details_schema = ApiError.model_json_schema()["properties"]["details"]

    assert {entry.get("type") for entry in details_schema["anyOf"]} >= {"string"}
    assert any("$ref" in entry for entry in details_schema["anyOf"])
    definitions = ApiError.model_json_schema()["$defs"]
    assert "ApiErrorDetails" in definitions
    assert "repair_template" in definitions["ApiErrorDetails"]["properties"]
