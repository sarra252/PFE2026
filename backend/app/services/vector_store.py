from uuid import NAMESPACE_URL, uuid5

from qdrant_client import QdrantClient
from qdrant_client.http import models


def get_qdrant_client(url: str, api_key: str = "") -> QdrantClient:
    if api_key:
        return QdrantClient(url=url, api_key=api_key)
    return QdrantClient(url=url)


def ensure_collection(
    client: QdrantClient,
    collection_name: str,
    vector_size: int,
    distance: models.Distance = models.Distance.COSINE,
) -> None:
    try:
        client.get_collection(collection_name=collection_name)
    except Exception:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=vector_size, distance=distance),
        )


def recreate_collection(
    client: QdrantClient,
    collection_name: str,
    vector_size: int,
    distance: models.Distance = models.Distance.COSINE,
) -> None:
    try:
        client.delete_collection(collection_name=collection_name)
    except Exception:
        pass
    ensure_collection(client, collection_name=collection_name, vector_size=vector_size, distance=distance)


def _stable_point_id(source: str) -> str:
    return str(uuid5(NAMESPACE_URL, source))


def upsert_documents(
    client: QdrantClient,
    collection_name: str,
    documents: list[dict],
    vectors: list[list[float]],
) -> None:
    points: list[models.PointStruct] = []
    for doc, vector in zip(documents, vectors):
        payload = {k: v for k, v in doc.items() if k != "id"}
        point_id = str(doc.get("id") or _stable_point_id(doc["source"]))
        points.append(models.PointStruct(id=point_id, vector=vector, payload=payload))

    if points:
        client.upsert(collection_name=collection_name, points=points, wait=True)


def search_documents(
    client: QdrantClient,
    collection_name: str,
    query_vector: list[float],
    limit: int = 4,
) -> list[dict]:
    response = client.query_points(
        collection_name=collection_name,
        query=query_vector,
        limit=limit,
    )

    points = getattr(response, "points", response)

    matches: list[dict] = []
    for result in points:
        payload = result.payload or {}
        text = str(payload.get("text", ""))
        matches.append(
            {
                "id": str(getattr(result, "id", "")),
                "source": str(payload.get("source", "unknown")),
                "source_type": str(payload.get("source_type", "unknown")),
                "score": float(getattr(result, "score", 0.0)),
                "text": text,
                "snippet": str(payload.get("snippet", text[:300])),
            }
        )
    return matches
