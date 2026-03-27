import json
import re
from pathlib import Path


class RagRetrievalError(ValueError):
    pass


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


def _safe_read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _load_documents(metadata_dir: Path) -> list[dict]:
    docs: list[dict] = []

    table_catalog_path = metadata_dir / "table_catalog.json"
    if table_catalog_path.exists():
        table_catalog = json.loads(table_catalog_path.read_text(encoding="utf-8"))
        for table_name, payload in table_catalog.items():
            text = f"table {table_name} " + json.dumps(payload, ensure_ascii=True)
            docs.append({"source": f"table_catalog:{table_name}", "text": text})

    synonyms_path = metadata_dir / "column_synonyms.json"
    if synonyms_path.exists():
        synonyms = json.loads(synonyms_path.read_text(encoding="utf-8"))
        for group, values in synonyms.items():
            text = f"synonyms {group} " + " ".join(str(v) for v in values)
            docs.append({"source": f"column_synonyms:{group}", "text": text})

    business_rules = _safe_read(metadata_dir / "business_rules.md")
    if business_rules.strip():
        for idx, line in enumerate([l.strip() for l in business_rules.splitlines() if l.strip()]):
            docs.append({"source": f"business_rules:{idx+1}", "text": line})

    query_library = _safe_read(metadata_dir / "query_library.sql")
    if query_library.strip():
        statements = [s.strip() for s in query_library.split(";") if s.strip()]
        for idx, stmt in enumerate(statements):
            docs.append({"source": f"query_library:{idx+1}", "text": stmt})

    return docs


def retrieve_rag_context(question: str, metadata_dir: str, top_k: int = 4) -> dict:
    base_dir = Path(metadata_dir)
    if not base_dir.exists() or not base_dir.is_dir():
        return {
            "context": "",
            "hits": [],
            "sources": [],
            "metadata_dir": str(base_dir),
            "warnings": [f"Repertoire metadata introuvable: {base_dir}"],
        }

    documents = _load_documents(base_dir)
    if not documents:
        return {
            "context": "",
            "hits": [],
            "sources": [],
            "metadata_dir": str(base_dir),
            "warnings": ["Aucun document RAG charge."],
        }

    q_tokens = _tokenize(question)
    scored: list[dict] = []
    for doc in documents:
        d_tokens = _tokenize(doc["text"])
        overlap = q_tokens.intersection(d_tokens)
        score = len(overlap)
        if score > 0:
            snippet = doc["text"][:300]
            scored.append({"source": doc["source"], "score": score, "snippet": snippet})

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

