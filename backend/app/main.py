import logging
import time
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import settings
from .logging_utils import configure_logging
from .models import (
    DocumentSqlRequest,
    GenerateSqlRequest,
    OptimizeSqlRequest,
    RagContextRequest,
    RunSqlRequest,
)
from .response import error_response, ok_response
from .services.documentation import document_sql
from .services.llm_provider_openai import OpenAIProviderError
from .services.llm_router import generate_sql_with_provider
from .services.offline_db import OfflineExecutionError, run_readonly_sql_offline
from .services.optimization import optimize_sql
from .services.rag_retriever import retrieve_rag_context
from .services.sql_safety import SqlValidationError, validate_sql_readonly
from .services.teradata_db import (
    TeradataConfigError,
    TeradataExecutionError,
    run_readonly_sql_teradata,
    teradata_config_from_settings,
)

configure_logging(settings.log_level)
logger = logging.getLogger("teradata_sql_copilot")

app = FastAPI(title=settings.app_name, version=settings.app_version)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    start = time.perf_counter()

    response = await call_next(request)

    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time-Ms"] = str(duration_ms)

    logger.info(
        "request_completed",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )
    return response


def verify_api_key(x_api_key: str = Header(default="")) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="API key invalide")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "unknown")
    payload = error_response(request_id, "http_error", str(exc.detail))
    return JSONResponse(status_code=exc.status_code, content=payload.model_dump())


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    request_id = getattr(request.state, "request_id", "unknown")
    payload = error_response(request_id, "validation_error", "Entree invalide")
    return JSONResponse(status_code=422, content=payload.model_dump())


@app.exception_handler(SqlValidationError)
async def sql_validation_handler(request: Request, exc: SqlValidationError):
    request_id = getattr(request.state, "request_id", "unknown")
    payload = error_response(request_id, "sql_safety_error", str(exc))
    return JSONResponse(status_code=400, content=payload.model_dump())


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    logger.exception("unhandled_exception", extra={"request_id": request_id})
    payload = error_response(request_id, "internal_error", "Erreur interne")
    return JSONResponse(status_code=500, content=payload.model_dump())


@app.get("/health")
def health():
    return {"status": "ok", "service": settings.app_name, "version": settings.app_version}


@app.get("/version")
def version():
    return {"version": settings.app_version}


@app.get("/db-health")
def db_health(_: None = Depends(verify_api_key)):
    backend_mode = (settings.db_backend or "offline").strip().lower()
    if backend_mode == "offline":
        return {
            "status": "ok",
            "backend": "offline",
            "offline_data_dir": settings.offline_data_dir,
        }

    if backend_mode == "teradata":
        try:
            td_config = teradata_config_from_settings(settings)
            probe = run_readonly_sql_teradata("SELECT 1 AS ok_value", td_config, row_limit=1)
            return {
                "status": "ok",
                "backend": "teradata",
                "row_count": probe.get("row_count", 0),
            }
        except (TeradataExecutionError, TeradataConfigError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    raise HTTPException(
        status_code=400,
        detail=f"DB_BACKEND invalide: {settings.db_backend}. Utilisez offline ou teradata.",
    )


@app.post("/rag-context")
def rag_context_endpoint(payload: RagContextRequest, _: None = Depends(verify_api_key), request: Request = None):
    rag = retrieve_rag_context(
        question=payload.question,
        metadata_dir=settings.rag_metadata_dir,
        top_k=payload.top_k,
    )
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    response = ok_response(request_id, rag, rag.get("warnings", []))
    return response


@app.post("/generate-sql")
def generate_sql_endpoint(payload: GenerateSqlRequest, _: None = Depends(verify_api_key), request: Request = None):
    rag = retrieve_rag_context(
        question=payload.question,
        metadata_dir=settings.rag_metadata_dir,
        top_k=4,
    )
    combined_context = "\n\n".join(x for x in [payload.schema_context, rag.get("context", "")] if x)
    effective_context = combined_context if combined_context else None

    try:
        result, model_used, llm_warnings = generate_sql_with_provider(
            question=payload.question,
            schema_context=effective_context,
            llm_mode=settings.llm_mode,
            openai_api_key=settings.openai_api_key,
            openai_model=settings.openai_model,
            openai_base_url=settings.openai_base_url,
            llm_timeout_s=settings.llm_timeout_s,
            llm_fallback_to_mock=settings.llm_fallback_to_mock,
        )
    except OpenAIProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    warnings = validate_sql_readonly(result.sql)
    warnings.extend(rag.get("warnings", []))
    warnings.extend(llm_warnings)
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))

    response = ok_response(
        request_id,
        {
            "sql": result.sql,
            "explanation": result.explanation,
            "confidence": result.confidence,
            "rag_hit_count": len(rag.get("hits", [])),
            "rag_sources": rag.get("sources", []),
            "model_used": model_used,
        },
        warnings,
    )
    return response


@app.post("/document-sql")
def document_sql_endpoint(payload: DocumentSqlRequest, _: None = Depends(verify_api_key), request: Request = None):
    warnings = validate_sql_readonly(payload.sql)
    result = document_sql(payload.sql)
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    response = ok_response(request_id, result, warnings)
    return response


@app.post("/optimize-sql")
def optimize_sql_endpoint(payload: OptimizeSqlRequest, _: None = Depends(verify_api_key), request: Request = None):
    validate_sql_readonly(payload.sql)
    result = optimize_sql(payload.sql)
    warnings = validate_sql_readonly(result["optimized_sql"])
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    response = ok_response(request_id, result, warnings)
    return response


@app.post("/run-sql")
def run_sql_endpoint(payload: RunSqlRequest, _: None = Depends(verify_api_key), request: Request = None):
    warnings = validate_sql_readonly(payload.sql)
    backend_mode = (settings.db_backend or "offline").strip().lower()

    try:
        if backend_mode == "offline":
            result = run_readonly_sql_offline(
                sql=payload.sql,
                data_dir=settings.offline_data_dir,
                row_limit=payload.row_limit,
            )
        elif backend_mode == "teradata":
            td_config = teradata_config_from_settings(settings)
            result = run_readonly_sql_teradata(
                sql=payload.sql,
                config=td_config,
                row_limit=payload.row_limit,
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"DB_BACKEND invalide: {settings.db_backend}. Utilisez offline ou teradata.",
            )
    except (OfflineExecutionError, TeradataExecutionError, TeradataConfigError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    response = ok_response(request_id, result, warnings)
    return response
