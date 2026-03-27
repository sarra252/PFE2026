import json
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app.config import settings
from backend.app.main import app
from backend.app.services.sql_safety import validate_sql_readonly


client = TestClient(app)
HEADERS = {"x-api-key": "changeme"}


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"


def test_db_health_offline():
    old_backend = settings.db_backend
    settings.db_backend = "offline"
    try:
        response = client.get("/db-health", headers=HEADERS)
    finally:
        settings.db_backend = old_backend

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["backend"] == "offline"


def test_generate_sql_success():
    payload = {"question": "Top 10 clients par chiffre d'affaires"}
    response = client.post("/generate-sql", headers=HEADERS, json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert "sql" in body["data"]
    assert body["data"]["confidence"] > 0
    assert "rag_hit_count" in body["data"]
    assert "rag_sources" in body["data"]


def test_generate_sql_unauthorized():
    response = client.post("/generate-sql", headers={"x-api-key": "wrong"}, json={"question": "CA par mois"})
    assert response.status_code == 401


def test_document_sql_success():
    payload = {
        "sql": "SELECT c.customer_id, i.amount FROM customers c JOIN invoices i ON c.customer_id = i.customer_id"
    }
    response = client.post("/document-sql", headers=HEADERS, json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert "tables" in body["data"]


def test_optimize_sql_replaces_select_star():
    payload = {"sql": "SELECT * FROM customers"}
    response = client.post("/optimize-sql", headers=HEADERS, json=payload)

    assert response.status_code == 200
    body = response.json()
    assert "SELECT *" not in body["data"]["optimized_sql"].upper()


def test_sql_safety_blocks_dangerous_statement():
    try:
        validate_sql_readonly("DELETE FROM invoices WHERE invoice_id = 'I000000001'")
        assert False, "Expected safety validation to fail"
    except Exception as exc:
        assert "lecture" in str(exc).lower() or "interdits" in str(exc).lower()


def test_sql_safety_blocks_multiple_statements():
    response = client.post(
        "/document-sql",
        headers=HEADERS,
        json={"sql": "SELECT 1; SELECT 2;"},
    )
    assert response.status_code == 400


def test_response_has_request_id_and_timing_header():
    response = client.post("/generate-sql", headers=HEADERS, json={"question": "CA par mois"})
    assert response.status_code == 200
    assert response.headers.get("X-Request-ID") is not None
    assert response.headers.get("X-Process-Time-Ms") is not None


def test_regression_reference_prompts():
    prompts_file = Path("data_examples/reference_prompts.json")
    prompts = json.loads(prompts_file.read_text(encoding="utf-8"))

    for item in prompts:
        response = client.post(
            "/generate-sql",
            headers=HEADERS,
            json={"question": item["question"]},
        )
        assert response.status_code == 200
        sql = response.json()["data"]["sql"].upper()
        assert sql.strip() != ""
        for forbidden in [" DROP ", " DELETE ", " UPDATE ", " INSERT ", " MERGE ", " ALTER "]:
            assert forbidden not in f" {sql} "


def test_validation_error_shape():
    response = client.post("/generate-sql", headers=HEADERS, json={"question": "abc"})
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "error"
    assert len(body["errors"]) >= 1


def test_run_sql_success_from_csv(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "customers.csv").write_text(
        "customer_id,customer_name,region\nC0000001,Alice,NORTH\nC0000002,Bob,SOUTH\n",
        encoding="utf-8",
    )

    old = settings.offline_data_dir
    settings.offline_data_dir = str(raw_dir)
    try:
        payload = {"sql": "SELECT customer_id, customer_name FROM customers", "row_limit": 10}
        response = client.post("/run-sql", headers=HEADERS, json=payload)
    finally:
        settings.offline_data_dir = old

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["source_format"] == "csv"
    assert body["data"]["row_count"] == 2
    assert body["data"]["columns"] == ["customer_id", "customer_name"]


def test_run_sql_returns_400_if_data_dir_missing(tmp_path):
    missing_dir = tmp_path / "does_not_exist"

    old = settings.offline_data_dir
    settings.offline_data_dir = str(missing_dir)
    try:
        payload = {"sql": "SELECT 1 AS x", "row_limit": 5}
        response = client.post("/run-sql", headers=HEADERS, json=payload)
    finally:
        settings.offline_data_dir = old

    assert response.status_code == 400
    body = response.json()
    assert body["status"] == "error"


def test_run_sql_uses_teradata_backend_when_configured(monkeypatch):
    old_backend = settings.db_backend
    settings.db_backend = "teradata"
    try:
        monkeypatch.setattr("backend.app.main.teradata_config_from_settings", lambda _: object())
        monkeypatch.setattr(
            "backend.app.main.run_readonly_sql_teradata",
            lambda sql, config, row_limit: {
                "columns": ["x"],
                "rows": [[1]],
                "row_count": 1,
                "row_limit": row_limit,
                "source_format": "teradata",
                "tables_loaded": [],
            },
        )
        response = client.post("/run-sql", headers=HEADERS, json={"sql": "SELECT 1 AS x", "row_limit": 25})
    finally:
        settings.db_backend = old_backend

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["source_format"] == "teradata"
    assert body["data"]["row_limit"] == 25


def test_db_health_uses_teradata_backend_when_configured(monkeypatch):
    old_backend = settings.db_backend
    settings.db_backend = "teradata"
    try:
        monkeypatch.setattr("backend.app.main.teradata_config_from_settings", lambda _: object())
        monkeypatch.setattr(
            "backend.app.main.run_readonly_sql_teradata",
            lambda sql, config, row_limit: {
                "columns": ["ok_value"],
                "rows": [[1]],
                "row_count": 1,
                "row_limit": row_limit,
                "source_format": "teradata",
                "tables_loaded": [],
            },
        )
        response = client.get("/db-health", headers=HEADERS)
    finally:
        settings.db_backend = old_backend

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["backend"] == "teradata"


def test_rag_context_endpoint_with_temp_metadata(tmp_path):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "table_catalog.json").write_text(
        '{"subscribers": {"pk": "subscriber_id", "potential_fk": "client_id -> customers.customer_id"}}',
        encoding="utf-8",
    )
    (metadata_dir / "column_synonyms.json").write_text(
        '{"customer_identifiers": ["customer_id", "client_id"], "subscriber_identifiers": ["subscriber_id", "msisdn"]}',
        encoding="utf-8",
    )
    (metadata_dir / "business_rules.md").write_text("Missing FK and noisy IDs", encoding="utf-8")
    (metadata_dir / "query_library.sql").write_text("SELECT subscriber_id, client_id FROM subscribers;", encoding="utf-8")

    old = settings.rag_metadata_dir
    settings.rag_metadata_dir = str(metadata_dir)
    try:
        payload = {"question": "comment relier client_id et subscriber_id ?", "top_k": 4}
        response = client.post("/rag-context", headers=HEADERS, json=payload)
    finally:
        settings.rag_metadata_dir = old

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["context"] != ""
    assert len(body["data"]["hits"]) >= 1


def test_generate_sql_uses_rag_context_endpoint_fields(tmp_path):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "table_catalog.json").write_text('{"customers": {"pk": "customer_id"}}', encoding="utf-8")
    (metadata_dir / "column_synonyms.json").write_text('{"customer_identifiers": ["customer_id", "client_id"]}', encoding="utf-8")
    (metadata_dir / "business_rules.md").write_text("Use customer_id as canonical key", encoding="utf-8")
    (metadata_dir / "query_library.sql").write_text("SELECT customer_id FROM customers;", encoding="utf-8")

    old = settings.rag_metadata_dir
    settings.rag_metadata_dir = str(metadata_dir)
    try:
        response = client.post(
            "/generate-sql",
            headers=HEADERS,
            json={"question": "Top clients", "schema_context": "focus clients"},
        )
    finally:
        settings.rag_metadata_dir = old

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert "rag_hit_count" in body["data"]
    assert "rag_sources" in body["data"]


def test_end_to_end_generate_then_run_sql_on_synthetic_like_data(tmp_path):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "table_catalog.json").write_text('{"invoices": {"pk": "invoice_id"}}', encoding="utf-8")
    (metadata_dir / "column_synonyms.json").write_text('{"customer_identifiers": ["customer_id", "client_id"]}', encoding="utf-8")
    (metadata_dir / "business_rules.md").write_text("monthly revenue based on invoices", encoding="utf-8")
    (metadata_dir / "query_library.sql").write_text("SELECT billing_month, SUM(amount) AS revenue FROM invoices GROUP BY 1;", encoding="utf-8")

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "invoices.csv").write_text(
        "invoice_id,customer_id,billing_month,amount\nI1,C1,2025-01,100\nI2,C2,2025-01,50\nI3,C1,2025-02,80\n",
        encoding="utf-8",
    )

    old_rag = settings.rag_metadata_dir
    old_data = settings.offline_data_dir
    settings.rag_metadata_dir = str(metadata_dir)
    settings.offline_data_dir = str(raw_dir)
    try:
        gen = client.post("/generate-sql", headers=HEADERS, json={"question": "CA par mois"})
        assert gen.status_code == 200
        sql = gen.json()["data"]["sql"]

        run = client.post("/run-sql", headers=HEADERS, json={"sql": sql, "row_limit": 50})
    finally:
        settings.rag_metadata_dir = old_rag
        settings.offline_data_dir = old_data

    assert run.status_code == 200
    body = run.json()
    assert body["status"] == "success"
    assert body["data"]["row_count"] >= 1


def test_generate_sql_openai_mode_without_key_falls_back_to_mock():
    old_mode = settings.llm_mode
    old_key = settings.openai_api_key
    old_fb = settings.llm_fallback_to_mock
    settings.llm_mode = "openai"
    settings.openai_api_key = ""
    settings.llm_fallback_to_mock = True
    try:
        response = client.post("/generate-sql", headers=HEADERS, json={"question": "CA par mois"})
    finally:
        settings.llm_mode = old_mode
        settings.openai_api_key = old_key
        settings.llm_fallback_to_mock = old_fb

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"].get("model_used") == "mock_fallback"
    assert any("Fallback mock active" in w for w in body.get("warnings", []))
