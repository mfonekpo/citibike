import json
import logging
import math
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import ollama
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    UpdateStatus,
    VectorParams,
)

from rag_system.ingestion.monthly_tripcount import (
    build_rag_document,
    generate_monthly_tripcount,
)
from rag_system.quardrails.validation import (
    METRIC,
    SOURCE_TABLE,
    MonthlyTripCount,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)

QDRANT_ENDPOINT = os.getenv("QDRANT_ENDPOINT")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")

COLLECTION_NAME = "citibike_monthly_tripCount"
EMBEDDING_MODEL = "nomic-embed-text"


def _prepare_document() -> dict[str, Any]:
    """Extract one warehouse snapshot and build its document once."""

    data = generate_monthly_tripcount()

    if not isinstance(data, dict):
        raise ValueError(
            "The ClickHouse extraction must return a dictionary."
        )

    rows = data.get("rows")

    if not isinstance(rows, list) or not rows:
        raise ValueError(
            "No monthly rows were extracted. "
            "Existing Qdrant data is unchanged."
        )

    metadata = data.get("extraction_metadata")

    if (
        not isinstance(metadata, dict)
        or metadata.get("source_table") != SOURCE_TABLE
        or metadata.get("metric") != METRIC
    ):
        raise ValueError(
            "The extraction source or metric is invalid."
        )

    validated_rows = [
        MonthlyTripCount.model_validate(row).model_dump()
        for row in rows
    ]

    months = [
        row["month"]
        for row in validated_rows
    ]

    if len(months) != len(set(months)):
        raise ValueError(
            "The extraction returned duplicate month rows."
        )

    snapshot = {
        **data,
        "rows": sorted(
            validated_rows,
            key=lambda row: row["month"],
        ),
    }

    document = build_rag_document(snapshot)

    if not isinstance(document, dict):
        raise ValueError(
            "The document builder must return a dictionary."
        )

    point_id = document.get("document_id")

    if not isinstance(point_id, str):
        raise ValueError(
            "The document ID must be a UUID string."
        )

    UUID(point_id)

    payload = document.get("payload")

    if not isinstance(payload, dict):
        raise ValueError(
            "The document payload is missing."
        )

    if (
        payload.get("source_table") != SOURCE_TABLE
        or payload.get("metric") != METRIC
        or payload.get("rows") != snapshot["rows"]
    ):
        raise ValueError(
            "The document payload differs from its extraction."
        )

    if (
        not isinstance(payload.get("text"), str)
        or not payload["text"].strip()
    ):
        raise ValueError(
            "The document text is empty."
        )

    document["payload"] = {
        **payload,
        "embedding_model": EMBEDDING_MODEL,
    }

    # Confirm the payload can be represented as valid JSON.
    json.dumps(
        document["payload"],
        allow_nan=False,
    )

    return document


def embed_document(
    document: dict[str, Any],
) -> list[float]:
    """Embed the exact document already prepared by the caller."""

    response = ollama.embed(
        model=EMBEDDING_MODEL,
        input=document["payload"]["text"],
        truncate=False,
    )

    vectors = response.embeddings

    if len(vectors) != 1 or not vectors[0]:
        raise RuntimeError(
            "Ollama must return one nonempty embedding."
        )

    vector = vectors[0]

    if not all(
        math.isfinite(value)
        for value in vector
    ):
        raise RuntimeError(
            "The embedding contains nonfinite values."
        )

    if not any(value != 0 for value in vector):
        raise RuntimeError(
            "The embedding cannot be an all-zero vector."
        )

    return vector


def _ensure_collection(
    client: QdrantClient,
    vector_size: int,
) -> None:
    """Create a missing collection or validate its configuration."""

    if not client.collection_exists(
        collection_name=COLLECTION_NAME
    ):
        created = client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),
        )

        if not created:
            raise RuntimeError(
                "Qdrant did not confirm collection creation."
            )

    info = client.get_collection(
        collection_name=COLLECTION_NAME
    )

    vectors = info.config.params.vectors

    if not isinstance(vectors, VectorParams):
        raise RuntimeError(
            "This ingestion job requires one unnamed vector."
        )

    if (
        vectors.size != vector_size
        or vectors.distance != Distance.COSINE
    ):
        raise RuntimeError(
            "The collection dimension or distance is incompatible. "
            "Use a new compatible collection for an intentional "
            "model change."
        )


def upload_vector_to_qdrant() -> dict[str, Any]:
    """Refresh the monthly snapshot and verify its stored payload."""

    if not QDRANT_ENDPOINT:
        raise RuntimeError(
            "QDRANT_ENDPOINT is missing from your .env file."
        )

    document = _prepare_document()
    vector = embed_document(document)
    payload = document["payload"]

    qdrant = QdrantClient(
        url=QDRANT_ENDPOINT,
        api_key=QDRANT_API_KEY,
        timeout=30,
    )

    try:
        _ensure_collection(
            qdrant,
            len(vector),
        )

        operation = qdrant.upsert(
            collection_name=COLLECTION_NAME,
            points=[
                PointStruct(
                    id=document["document_id"],
                    vector=vector,
                    payload=payload,
                )
            ],
            wait=True,
        )

        if operation.status != UpdateStatus.COMPLETED:
            raise RuntimeError(
                "Qdrant did not confirm completion: "
                f"{operation.status}"
            )

        stored = qdrant.retrieve(
            collection_name=COLLECTION_NAME,
            ids=[document["document_id"]],
            with_payload=True,
            with_vectors=False,
        )

        if (
            len(stored) != 1
            or stored[0].payload != payload
        ):
            raise RuntimeError(
                "Stored payload verification failed."
            )

    finally:
        qdrant.close()

    summary = {
        "status": "completed",
        "collection": COLLECTION_NAME,
        "document_id": document["document_id"],
        "months": [
            row["month"]
            for row in payload["rows"]
        ],
        "month_count": len(payload["rows"]),
        "total_trips": sum(
            row["trip_count"]
            for row in payload["rows"]
        ),
        "vector_dimension": len(vector),
    }

    logger.info(
        "Embedding refresh completed: %s",
        summary,
    )

    return summary


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    print(
        json.dumps(
            upload_vector_to_qdrant(),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()