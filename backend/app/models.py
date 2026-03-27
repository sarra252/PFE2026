from typing import Any

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    code: str
    message: str


class ApiResponse(BaseModel):
    request_id: str
    status: str
    data: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    errors: list[ErrorDetail] = Field(default_factory=list)


class GenerateSqlRequest(BaseModel):
    question: str = Field(min_length=5)
    schema_context: str | None = None


class GenerateSqlData(BaseModel):
    sql: str
    explanation: str
    confidence: float


class DocumentSqlRequest(BaseModel):
    sql: str = Field(min_length=10)


class DocumentSqlData(BaseModel):
    summary: str
    tables: list[str]
    columns: list[str]
    clause_logic: list[str]


class OptimizeSqlRequest(BaseModel):
    sql: str = Field(min_length=10)


class OptimizeSqlData(BaseModel):
    optimized_sql: str
    rationale: list[str]
    cautions: list[str]


class RunSqlRequest(BaseModel):
    sql: str = Field(min_length=10)
    row_limit: int = Field(default=100, ge=1, le=5000)

class RagContextRequest(BaseModel):
    question: str = Field(min_length=5)
    top_k: int = Field(default=4, ge=1, le=10)
