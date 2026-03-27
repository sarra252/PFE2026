#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from typing import Any

import httpx
import streamlit as st


def api_get(base_url: str, endpoint: str, api_key: str, timeout_s: int) -> tuple[int, dict[str, Any] | str]:
    url = f"{base_url.rstrip('/')}{endpoint}"
    headers = {"x-api-key": api_key}
    try:
        with httpx.Client(timeout=timeout_s) as client:
            response = client.get(url, headers=headers)
        try:
            body = response.json()
        except Exception:
            body = response.text
        return response.status_code, body
    except Exception as exc:
        return 0, f"Erreur reseau/API: {exc}"


def api_post(base_url: str, endpoint: str, api_key: str, payload: dict[str, Any], timeout_s: int) -> tuple[int, dict[str, Any] | str]:
    url = f"{base_url.rstrip('/')}{endpoint}"
    headers = {"x-api-key": api_key}
    try:
        with httpx.Client(timeout=timeout_s) as client:
            response = client.post(url, headers=headers, json=payload)
        try:
            body = response.json()
        except Exception:
            body = response.text
        return response.status_code, body
    except Exception as exc:
        return 0, f"Erreur reseau/API: {exc}"


def render_api_result(status_code: int, body: dict[str, Any] | str) -> None:
    if status_code == 0:
        st.error(str(body))
        return

    if isinstance(body, dict):
        if status_code >= 400:
            st.error(f"HTTP {status_code}")
            st.json(body)
        else:
            st.success(f"HTTP {status_code}")
            st.json(body)
    else:
        if status_code >= 400:
            st.error(f"HTTP {status_code}: {body}")
        else:
            st.info(body)


def main() -> None:
    default_base_url = os.getenv("STREAMLIT_API_BASE_URL", "http://127.0.0.1:8000")
    default_api_key = os.getenv("STREAMLIT_API_KEY", os.getenv("API_KEY", "changeme"))

    st.set_page_config(page_title="Teradata SQL Copilot - Demo UI", layout="wide")
    st.title("Teradata SQL Copilot - Mini UI")
    st.caption("Demo interface for generate / optimize / document / run SQL / inspect RAG")

    with st.sidebar:
        st.header("Configuration API")
        base_url = st.text_input("Base URL", value=default_base_url)
        api_key = st.text_input("API Key", value=default_api_key, type="password")
        timeout_s = st.number_input("Timeout (s)", min_value=5, max_value=120, value=40, step=5)

        if st.button("Health Check"):
            status, body = api_get(base_url, "/health", api_key, int(timeout_s))
            render_api_result(status, body)

        if st.button("DB Health Check"):
            status, body = api_get(base_url, "/db-health", api_key, int(timeout_s))
            render_api_result(status, body)

        with st.expander("Quick Status", expanded=True):
            db_status, db_body = api_get(base_url, "/db-health", api_key, int(timeout_s))
            if db_status == 200 and isinstance(db_body, dict):
                st.success(f"DB: {db_body.get('backend', 'unknown')}")
                if "row_count" in db_body:
                    st.caption(f"db-health row_count={db_body.get('row_count')}")
            elif db_status == 0:
                st.warning("DB status unavailable (API down)")
            else:
                st.warning("DB status unavailable")

    if "last_generated_sql" not in st.session_state:
        st.session_state.last_generated_sql = ""
    if "last_optimized_sql" not in st.session_state:
        st.session_state.last_optimized_sql = ""

    tabs = st.tabs([
        "Generate SQL",
        "Optimize SQL",
        "Document SQL",
        "Run SQL",
        "RAG Debug",
    ])

    with tabs[0]:
        st.subheader("Natural Language -> SQL")
        question = st.text_area(
            "Question metier",
            value="Top 10 clients par chiffre d'affaires",
            height=120,
        )
        schema_context = st.text_area(
            "Schema context (optionnel)",
            value="",
            height=120,
            placeholder="Ajoute ici un contexte metier/schema si besoin",
        )

        if st.button("Generate SQL", type="primary"):
            payload = {"question": question}
            if schema_context.strip():
                payload["schema_context"] = schema_context
            status, body = api_post(base_url, "/generate-sql", api_key, payload, int(timeout_s))

            if status == 200 and isinstance(body, dict) and body.get("data", {}).get("sql"):
                st.session_state.last_generated_sql = body["data"]["sql"]

                st.code(body["data"]["sql"], language="sql")
                st.write(f"Confidence: {body['data'].get('confidence')}")
                st.write(f"Model used: {body['data'].get('model_used', 'unknown')}")
                st.write(f"RAG hits: {body['data'].get('rag_hit_count', 0)}")
                st.write(body["data"].get("explanation", ""))
                if body["data"].get("rag_sources"):
                    st.write("RAG sources:")
                    st.write("\n".join(f"- {s}" for s in body["data"]["rag_sources"]))
                if body.get("warnings"):
                    st.warning("Warnings:\n" + "\n".join(f"- {w}" for w in body["warnings"]))
            else:
                render_api_result(status, body)

    with tabs[1]:
        st.subheader("Optimize existing SQL")
        default_opt_sql = st.session_state.last_generated_sql or "SELECT * FROM sales.orders"
        sql_to_optimize = st.text_area("SQL a optimiser", value=default_opt_sql, height=220)

        if st.button("Optimize SQL"):
            payload = {"sql": sql_to_optimize}
            status, body = api_post(base_url, "/optimize-sql", api_key, payload, int(timeout_s))
            if status == 200 and isinstance(body, dict):
                optimized_sql = body.get("data", {}).get("optimized_sql", "")
                st.session_state.last_optimized_sql = optimized_sql
                st.code(optimized_sql, language="sql")

                rationale = body.get("data", {}).get("rationale", [])
                cautions = body.get("data", {}).get("cautions", [])
                if rationale:
                    st.write("Rationale:")
                    st.write("\n".join(f"- {x}" for x in rationale))
                if cautions:
                    st.warning("Cautions:\n" + "\n".join(f"- {x}" for x in cautions))
                if body.get("warnings"):
                    st.warning("Warnings:\n" + "\n".join(f"- {w}" for w in body["warnings"]))
            else:
                render_api_result(status, body)

    with tabs[2]:
        st.subheader("Document SQL")
        default_doc_sql = st.session_state.last_optimized_sql or st.session_state.last_generated_sql or "SELECT customer_id FROM customers"
        sql_to_doc = st.text_area("SQL a documenter", value=default_doc_sql, height=220)

        if st.button("Document SQL"):
            payload = {"sql": sql_to_doc}
            status, body = api_post(base_url, "/document-sql", api_key, payload, int(timeout_s))
            if status == 200 and isinstance(body, dict):
                data = body.get("data", {})
                st.write(f"Summary: {data.get('summary', '')}")
                st.write("Tables:")
                st.write("\n".join(f"- {x}" for x in data.get("tables", [])) or "-")
                st.write("Columns:")
                st.write("\n".join(f"- {x}" for x in data.get("columns", [])) or "-")
                st.write("Clause logic:")
                st.write("\n".join(f"- {x}" for x in data.get("clause_logic", [])) or "-")
                if body.get("warnings"):
                    st.warning("Warnings:\n" + "\n".join(f"- {w}" for w in body["warnings"]))
            else:
                render_api_result(status, body)

    with tabs[3]:
        st.subheader("Run SQL (Teradata online or offline)")
        default_run_sql = st.session_state.last_optimized_sql or st.session_state.last_generated_sql or "SELECT customer_id, customer_name FROM customers"
        sql_to_run = st.text_area("SQL a executer", value=default_run_sql, height=220)
        row_limit = st.slider("Row limit", min_value=1, max_value=5000, value=100)

        if st.button("Run SQL"):
            payload = {"sql": sql_to_run, "row_limit": row_limit}
            status, body = api_post(base_url, "/run-sql", api_key, payload, int(timeout_s))
            if status == 200 and isinstance(body, dict):
                data = body.get("data", {})
                st.write(f"Rows returned: {data.get('row_count')} (limit={data.get('row_limit')})")
                st.write(f"Source format: {data.get('source_format')}")
                st.write(f"Tables loaded: {', '.join(data.get('tables_loaded', []))}")

                columns = data.get("columns", [])
                rows = data.get("rows", [])
                if columns and rows:
                    preview_rows = [{col: row[idx] if idx < len(row) else None for idx, col in enumerate(columns)} for row in rows]
                    st.dataframe(preview_rows, use_container_width=True)
                else:
                    st.info("Aucune ligne retournee")

                if body.get("warnings"):
                    st.warning("Warnings:\n" + "\n".join(f"- {w}" for w in body["warnings"]))
            else:
                render_api_result(status, body)

    with tabs[4]:
        st.subheader("Inspect retrieved RAG context")
        rag_question = st.text_area("Question pour RAG", value="comment joindre client_id et subscriber_id ?", height=120)
        top_k = st.slider("top_k", min_value=1, max_value=10, value=4)

        if st.button("Get RAG context"):
            payload = {"question": rag_question, "top_k": top_k}
            status, body = api_post(base_url, "/rag-context", api_key, payload, int(timeout_s))
            if status == 200 and isinstance(body, dict):
                data = body.get("data", {})
                st.write("Sources:")
                st.write("\n".join(f"- {x}" for x in data.get("sources", [])) or "-")
                st.write("Context:")
                st.code(data.get("context", ""), language="markdown")
                st.write("Hits:")
                st.json(data.get("hits", []))
                if body.get("warnings"):
                    st.warning("Warnings:\n" + "\n".join(f"- {w}" for w in body["warnings"]))
            else:
                render_api_result(status, body)

    with st.expander("Raw response helper"):
        st.caption("Use this box if you want to paste any API JSON for formatting.")
        raw = st.text_area("Raw JSON", value="", height=120)
        if raw.strip():
            try:
                st.code(json.dumps(json.loads(raw), indent=2, ensure_ascii=False), language="json")
            except Exception:
                st.code(raw)


if __name__ == "__main__":
    main()
