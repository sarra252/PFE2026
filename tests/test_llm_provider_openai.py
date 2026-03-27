from backend.app.services.llm_provider_openai import _parse_confidence, _sanitize_sql_candidate


def test_parse_confidence_numeric():
    assert _parse_confidence(0.85) == 0.85
    assert _parse_confidence(2) == 1.0
    assert _parse_confidence(-1) == 0.0


def test_parse_confidence_lexical():
    assert _parse_confidence("High") == 0.9
    assert _parse_confidence("medium") == 0.7
    assert _parse_confidence("low") == 0.4


def test_parse_confidence_fallback():
    assert _parse_confidence("unknown") == 0.7
    assert _parse_confidence(None) == 0.7


def test_sanitize_sql_candidate_strips_prefix_text():
    raw = "Voici la requete SQL demandee: SELECT customer_id FROM customers ORDER BY customer_id DESC;"
    assert _sanitize_sql_candidate(raw).upper().startswith("SELECT")


def test_sanitize_sql_candidate_extracts_sql_from_fence():
    raw = "```sql\nSELECT customer_id FROM customers;\n```"
    assert _sanitize_sql_candidate(raw).strip() == "SELECT customer_id FROM customers;"
