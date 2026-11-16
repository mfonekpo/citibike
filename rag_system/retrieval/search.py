import os
from dotenv import load_dotenv
from typing import Any
import ollama
from qdrant_client import QdrantClient

load_dotenv()

QDRANT_ENDPOINT = os.getenv("QDRANT_ENDPOINT")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = "citibike_monthly_tripCount"

EMBEDDING_MODEL = "nomic-embed-text"  # Must match ingestion!
CHAT_MODEL = "qwen2.5:1.5b"

def retrieve_points(question: str, top_k: int = 3) -> list[dict[str, Any]]:
  embed_response = ollama.embed(model=EMBEDDING_MODEL, input=question)
  query_vector = embed_response.embeddings[0]

  qdrant = QdrantClient(url=QDRANT_ENDPOINT, api_key=QDRANT_API_KEY, timeout=15)
  search_result = qdrant.query_points(
      collection_name=COLLECTION_NAME,
      query=query_vector,
      with_payload=True,
      limit=top_k,
  )

  return [
      {
          "score": hit.score,
          "text": hit.payload.get("text", ""),
          "source_table": hit.payload.get("source_table", "unknown"),
          "extraction_id": hit.payload.get("extraction_id", "unknown"),
      }
      for hit in search_result.points
      if hit.payload
  ]


def ask_citibike_rag(question: str) -> dict[str, Any]:
    hits = retrieve_points(question)

    if not hits:
        return {
            "question": question,
            "understood_query": question,
            "answer": (
                "I don't have enough warehouse data to answer that."
            ),
            "sources": [],
        }

    context = "\n\n---\n\n".join(
        hit["text"] for hit in hits
    )

    response = ollama.chat(
        model=CHAT_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "Answer using only the provided warehouse context. "
                    "If the answer is missing, say you do not have "
                    "enough warehouse data. "
                    "Copy exact numbers from the context."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Context:\n{context}\n\n"
                    f"Question: {question}"
                ),
            },
        ],
        options={"temperature": 0.0},
    )

    answer = (response.message.content or "").strip()

    return {
        "question": question,
        "understood_query": question,
        "answer": answer or (
            "I don't have enough warehouse data to answer that."
        ),
        "sources": hits,
    }