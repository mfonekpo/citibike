import os
from dotenv import load_dotenv
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


def ask_citibike_rag(question: str) -> None:
  hits = retrieve_points(question)

  if not hits:
    print("No relevant Citibike data found.")
    return

  context = "\n\n---\n\n".join(hit["text"] for hit in hits)

  response = ollama.chat(
      model=CHAT_MODEL,
      messages=[
          {
              "role": "system",
              "content": (
                  "You are a precise Citibike data analyst. Answer the"
                  " question using ONLY the provided context. Copy exact"
                  " numbers from the context. If the answer is missing, say"
                  " 'I don't have enough data to answer that.'"
              ),
          },
          {
              "role": "user",
              "content": f"Context:\n{context}\n\nQuestion: {question}",
          },
      ],
      options={
          "temperature": 0.0  # Keeps qwen2.5:1.5b deterministic and factual
      },
  )

  print(f"Answer:\n{response.message.content}\n")
  print("Sources:")
  for hit in hits:
    print(
        f"- Table: {hit['source_table']} | ID: {hit['extraction_id']} (Score:"
        f" {hit['score']:.4f})"
    )


def main():
  question = "Which month had the most Citibike trips, and how many were there?"
  print(f"Question: {question}\n")
  ask_citibike_rag(question)


if __name__ == "__main__":
  main()