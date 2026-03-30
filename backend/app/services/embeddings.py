from functools import lru_cache

from sentence_transformers import SentenceTransformer


def _normalize_input(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _get_prefix(model_name: str, kind: str) -> str:
    lowered = (model_name or "").lower()
    if "e5" in lowered:
        return "query: " if kind == "query" else "passage: "
    return ""


@lru_cache(maxsize=4)
def get_embedding_model(model_name: str) -> SentenceTransformer:
    return SentenceTransformer(model_name)


def embed_texts(texts: list[str], model_name: str, kind: str = "document") -> list[list[float]]:
    if not texts:
        return []

    prefix = _get_prefix(model_name, kind)
    model = get_embedding_model(model_name)
    prepared = [prefix + _normalize_input(text) for text in texts]
    vectors = model.encode(prepared, normalize_embeddings=True)
    return [vector.tolist() for vector in vectors]


def embed_query(text: str, model_name: str) -> list[float]:
    vectors = embed_texts([text], model_name=model_name, kind="query")
    return vectors[0] if vectors else []
