import ollama
from rag_system.ingestion.monthly_tripcount import (
    generate_monthly_tripcount,
    build_rag_document,
)
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from dotenv import load_dotenv
import os

load_dotenv()

qdrant_endpoint = os.getenv("QDRANT_ENDPOINT")
qdrant_api_key = os.getenv("QDRANT_API_KEY")

def embed_document():
    data = generate_monthly_tripcount()
    document = build_rag_document(data)

    response = ollama.embed(
        model='nomic-embed-text',
        input=document["payload"]["text"],
        truncate=False
    )

    vector = response.embeddings
    return vector[0]


def upload_vector_to_qdrant():
    data = generate_monthly_tripcount()
    document = build_rag_document(data)
    qdrantClient = QdrantClient(
        url=qdrant_endpoint,
        api_key=qdrant_api_key,
        timeout=15,
        cloud_inference=True,
    )

    qdrantClient.upsert(
        collection_name="citibike_monthly_tripCount",
        points = [
            PointStruct(
                id=document["document_id"],
                vector=embed_document(),
                payload=document["payload"]
            )
        ]
    )


def main():
    upload_vector_to_qdrant()


if __name__ == "__main__":
    main()