import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.app.config import settings
from backend.app.services.embeddings import embed_texts
from backend.app.services.rag_retriever import load_metadata_documents
from backend.app.services.vector_store import ensure_collection, get_qdrant_client, recreate_collection, upsert_documents


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index local metadata files into Qdrant.")
    parser.add_argument("--metadata-dir", default=settings.rag_metadata_dir, help="Metadata directory to index.")
    parser.add_argument("--collection", default=settings.qdrant_collection, help="Target Qdrant collection.")
    parser.add_argument("--qdrant-url", default=settings.qdrant_url, help="Qdrant server URL.")
    parser.add_argument("--qdrant-api-key", default=settings.qdrant_api_key, help="Qdrant API key.")
    parser.add_argument("--embedding-model", default=settings.embedding_model, help="Embedding model name.")
    parser.add_argument("--recreate", action="store_true", help="Delete and recreate the collection before indexing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata_dir = Path(args.metadata_dir)
    if not metadata_dir.exists() or not metadata_dir.is_dir():
        print(f"[ERROR] Metadata directory not found: {metadata_dir}")
        return 1

    documents = load_metadata_documents(metadata_dir)
    if not documents:
        print(f"[ERROR] No RAG documents found in: {metadata_dir}")
        return 1

    vectors = embed_texts([doc["text"] for doc in documents], model_name=args.embedding_model, kind="document")
    if not vectors:
        print("[ERROR] Embedding generation failed.")
        return 1

    client = get_qdrant_client(url=args.qdrant_url, api_key=args.qdrant_api_key)
    vector_size = len(vectors[0])
    if args.recreate:
        recreate_collection(client, collection_name=args.collection, vector_size=vector_size)
    else:
        ensure_collection(client, collection_name=args.collection, vector_size=vector_size)

    upsert_documents(client, collection_name=args.collection, documents=documents, vectors=vectors)
    print(f"[OK] Indexed {len(documents)} documents into collection '{args.collection}' on {args.qdrant_url}")
    print(f"[INFO] Embedding model: {args.embedding_model}")
    print(f"[INFO] Vector size: {vector_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
