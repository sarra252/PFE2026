from typing import Any

from .models import ApiResponse, ErrorDetail


def ok_response(request_id: str, data: dict[str, Any], warnings: list[str] | None = None) -> ApiResponse:
    return ApiResponse(
        request_id=request_id,
        status="success",
        data=data,
        warnings=warnings or [],
        errors=[],
    )


def error_response(request_id: str, code: str, message: str, status: str = "error") -> ApiResponse:
    return ApiResponse(
        request_id=request_id,
        status=status,
        data={},
        warnings=[],
        errors=[ErrorDetail(code=code, message=message)],
    )