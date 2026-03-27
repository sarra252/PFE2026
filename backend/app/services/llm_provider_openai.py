import json
import re

from openai import OpenAI

from .mock_llm import LlmSqlResult


class OpenAIProviderError(RuntimeError):
    pass


def _parse_confidence(value: object) -> float:
    if isinstance(value, (int, float)):
        return max(0.0, min(1.0, float(value)))

    if isinstance(value, str):
        txt = value.strip().lower()
        lexical_map = {
            "very high": 0.95,
            "high": 0.9,
            "medium": 0.7,
            "low": 0.4,
            "very low": 0.2,
        }
        if txt in lexical_map:
            return lexical_map[txt]
        try:
            parsed = float(txt.replace(",", "."))
            return max(0.0, min(1.0, parsed))
        except Exception:
            return 0.7

    return 0.7


def _normalize_json_content(content: str) -> str:
    text = (content or "").strip()
    if not text:
        return text

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()
    return text


def _extract_sql_from_text(text: str) -> str:
    if not text:
        return ""

    fenced = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()

    generic_fenced = re.search(r"```\s*(.*?)```", text, flags=re.DOTALL)
    if generic_fenced:
        candidate = generic_fenced.group(1).strip()
        if re.search(r"\b(select|with)\b", candidate, flags=re.IGNORECASE):
            return candidate

    inline = re.search(r"\b(?:select|with)\b[\s\S]*", text, flags=re.IGNORECASE)
    if not inline:
        return ""

    sql = inline.group(0).strip()
    if ";" in sql:
        sql = sql.split(";", 1)[0].strip() + ";"
    return sql


def _sanitize_sql_candidate(sql: str) -> str:
    text = (sql or "").strip()
    if not text:
        return ""

    if text.startswith("```"):
        extracted = _extract_sql_from_text(text)
        if extracted:
            return extracted.strip()

    start = re.search(r"\b(select|with)\b", text, flags=re.IGNORECASE)
    if start and start.start() > 0:
        text = text[start.start():].strip()

    if ";" in text:
        text = text.split(";", 1)[0].strip() + ";"
    return text


def _payload_from_text_fallback(text: str) -> dict:
    sql = _extract_sql_from_text(text)
    explanation = text.strip()
    if len(explanation) > 600:
        explanation = explanation[:600].rstrip() + "..."
    if not explanation:
        explanation = "Generation SQL via provider OpenAI-compatible."
    return {"sql": sql, "explanation": explanation, "confidence": 0.7}


def generate_sql_with_openai(
    *,
    question: str,
    schema_context: str | None,
    api_key: str,
    model: str,
    base_url: str | None,
    timeout_s: int,
) -> LlmSqlResult:
    if not api_key.strip():
        raise OpenAIProviderError("OPENAI_API_KEY manquante.")

    client_kwargs = {
        "api_key": api_key,
        "timeout": timeout_s,
    }
    if base_url and base_url.strip():
        client_kwargs["base_url"] = base_url.strip()
    client = OpenAI(**client_kwargs)

    system_prompt = (
        "Tu es un assistant SQL expert Teradata. "
        "Genere UNE seule requete readonly (SELECT / WITH). "
        "Interdits: DROP, DELETE, UPDATE, INSERT, MERGE, ALTER, TRUNCATE, CREATE. "
        "Reponds strictement en JSON avec les cles: sql, explanation, confidence."
    )
    user_prompt = f"Question: {question}\n\nContexte: {schema_context or 'Aucun contexte'}"

    try:
        completion = client.chat.completions.create(
            model=model,
            temperature=0.1,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception as exc:
        raise OpenAIProviderError(f"Echec appel OpenAI-compatible API: {exc}") from exc

    if not completion.choices:
        raise OpenAIProviderError("Reponse provider vide (choices manquants).")

    content = completion.choices[0].message.content or ""
    content = _normalize_json_content(content)
    try:
        payload = json.loads(content)
    except Exception:
        payload = _payload_from_text_fallback(content)
    if not isinstance(payload, dict):
        raise OpenAIProviderError("Reponse provider JSON invalide: objet attendu.")

    sql = _sanitize_sql_candidate(str(payload.get("sql", "")))
    explanation = str(payload.get("explanation", "")).strip() or "Generation SQL via provider OpenAI-compatible."
    confidence = _parse_confidence(payload.get("confidence", 0.7))

    if not sql:
        raise OpenAIProviderError("Le provider a retourne un SQL vide.")

    return LlmSqlResult(sql=sql, explanation=explanation, confidence=confidence)
