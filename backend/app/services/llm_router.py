from .llm_provider_openai import OpenAIProviderError, generate_sql_with_openai
from .mock_llm import LlmSqlResult, generate_sql as generate_sql_mock


def generate_sql_with_provider(
    *,
    question: str,
    schema_context: str | None,
    llm_mode: str,
    openai_api_key: str,
    openai_model: str,
    openai_base_url: str,
    llm_timeout_s: int,
    llm_fallback_to_mock: bool,
) -> tuple[LlmSqlResult, str, list[str]]:
    mode = (llm_mode or "mock").lower().strip()

    if mode != "openai":
        return generate_sql_mock(question, schema_context), "mock", []

    try:
        result = generate_sql_with_openai(
            question=question,
            schema_context=schema_context,
            api_key=openai_api_key,
            model=openai_model,
            base_url=openai_base_url,
            timeout_s=llm_timeout_s,
        )
        return result, "openai", []
    except OpenAIProviderError as exc:
        if llm_fallback_to_mock:
            fallback = generate_sql_mock(question, schema_context)
            warning = f"Fallback mock active (OpenAI indisponible): {exc}"
            return fallback, "mock_fallback", [warning]
        raise
