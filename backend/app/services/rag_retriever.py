import json
import re
from pathlib import Path

from ..config import settings
from .embeddings import embed_query
from .vector_store import get_qdrant_client, search_documents


class RagRetrievalError(ValueError):
    pass


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


def _safe_read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def load_metadata_documents(metadata_dir: Path) -> list[dict]:
    docs: list[dict] = []

    table_catalog_path = metadata_dir / "table_catalog.json"
    if table_catalog_path.exists():
        table_catalog = json.loads(table_catalog_path.read_text(encoding="utf-8"))
        for table_name, payload in table_catalog.items():
            table_json = json.dumps(payload, ensure_ascii=True)
            text = f"table {table_name} {table_json}"
            docs.append(
                {
                    "source": f"table_catalog:{table_name}",
                    "source_type": "table_catalog",
                    "text": text,
                    "snippet": text[:300],
                }
            )

    synonyms_path = metadata_dir / "column_synonyms.json"
    if synonyms_path.exists():
        synonyms = json.loads(synonyms_path.read_text(encoding="utf-8"))
        for group, values in synonyms.items():
            text = f"synonyms {group} " + " ".join(str(v) for v in values)
            docs.append(
                {
                    "source": f"column_synonyms:{group}",
                    "source_type": "column_synonyms",
                    "text": text,
                    "snippet": text[:300],
                }
            )

    business_rules = _safe_read(metadata_dir / "business_rules.md")
    if business_rules.strip():
        for idx, line in enumerate([l.strip() for l in business_rules.splitlines() if l.strip()]):
            docs.append(
                {
                    "source": f"business_rules:{idx+1}",
                    "source_type": "business_rules",
                    "text": line,
                    "snippet": line[:300],
                }
            )

    query_library = _safe_read(metadata_dir / "query_library.sql")
    if query_library.strip():
        statements = [s.strip() for s in query_library.split(";") if s.strip()]
        for idx, stmt in enumerate(statements):
            docs.append(
                {
                    "source": f"query_library:{idx+1}",
                    "source_type": "query_library",
                    "text": stmt,
                    "snippet": stmt[:300],
                }
            )

    return docs


def _empty_result(metadata_dir: Path, warnings: list[str]) -> dict:
    return {
        "context": "",
        "hits": [],
        "sources": [],
        "metadata_dir": str(metadata_dir),
        "warnings": warnings,
    }


def _retrieve_local_context(question: str, metadata_dir: str, top_k: int = 4) -> dict:
    base_dir = Path(metadata_dir)
    if not base_dir.exists() or not base_dir.is_dir():
        return _empty_result(base_dir, [f"Repertoire metadata introuvable: {base_dir}"])

    documents = load_metadata_documents(base_dir)
    if not documents:
        return _empty_result(base_dir, ["Aucun document RAG charge."])

    q_tokens = _tokenize(question)
    scored: list[dict] = []
    for doc in documents:
        d_tokens = _tokenize(doc["text"])
        overlap = q_tokens.intersection(d_tokens)
        score = len(overlap)
        if score > 0:
            scored.append({"source": doc["source"], "score": score, "snippet": doc["snippet"]})

    scored.sort(key=lambda x: x["score"], reverse=True)
    hits = scored[:top_k]
    context = "\n\n".join(f"[{h['source']}] {h['snippet']}" for h in hits)
    sources = [h["source"] for h in hits]

    return {
        "context": context,
        "hits": hits,
        "sources": sources,
        "metadata_dir": str(base_dir),
        "warnings": [],
    }


def _retrieve_vector_context(question: str, metadata_dir: str, top_k: int = 4) -> dict:
    base_dir = Path(metadata_dir)
    if not base_dir.exists() or not base_dir.is_dir():
        return _empty_result(base_dir, [f"Repertoire metadata introuvable: {base_dir}"])

    query_vector = embed_query(question, model_name=settings.embedding_model)
    if not query_vector:
        return _empty_result(base_dir, ["Generation embedding impossible pour la question."])

    client = get_qdrant_client(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
    matches = search_documents(
        client,
        collection_name=settings.qdrant_collection,
        query_vector=query_vector,
        limit=top_k,
    )

    hits = [
        {
            "source": match["source"],
            "score": match["score"],
            "snippet": match["snippet"],
        }
        for match in matches
    ]
    context = "\n\n".join(f"[{hit['source']}] {hit['snippet']}" for hit in hits)
    sources = [hit["source"] for hit in hits]

    return {
        "context": context,
        "hits": hits,
        "sources": sources,
        "metadata_dir": str(base_dir),
        "warnings": [],
    }


def retrieve_rag_context(question: str, metadata_dir: str, top_k: int = 4) -> dict:
    backend = (settings.rag_backend or "local").strip().lower()
    effective_top_k = top_k or settings.rag_top_k

    if backend == "vector":
        try:
            return _retrieve_vector_context(question=question, metadata_dir=metadata_dir, top_k=effective_top_k)
        except Exception as exc:
            local_result = _retrieve_local_context(question=question, metadata_dir=metadata_dir, top_k=effective_top_k)
            local_result["warnings"].append(f"Qdrant indisponible, fallback local active: {exc}")
            return local_result

    return _retrieve_local_context(question=question, metadata_dir=metadata_dir, top_k=effective_top_k)
