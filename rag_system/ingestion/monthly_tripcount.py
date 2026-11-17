import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from rag_system.quardrails.validation import (
    METRIC,
    SOURCE_TABLE,
    MonthlyTripCount,
)
from utils.dwh_connect import get_clickhouse_client


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

REPORTING_TIMEZONE = "America/New_York"
DOCUMENT_KEY = "citibike:monthly_trip_count:all_loaded_months:v1"

# Preserve the UUID produced by existing extractor.
DOCUMENT_ID = "43b84bc7-2d1c-52cb-8a8e-b42f1ca5bc1b"

MONTHLY_TRIPCOUNT_QUERY = """
SELECT
    formatDateTime(
        started_at, '%Y-%m', 'America/New_York'
    ) AS month,
    count() AS trip_count
FROM fct_trips
GROUP BY month
ORDER BY month
"""


def _validate_rows(
    rows: Any,
) -> list[dict[str, Any]]:
    """Validate and sort one row per available month."""

    if not isinstance(rows, list) or not rows:
        raise ValueError(
            "No monthly trip-count rows were returned from ClickHouse."
        )

    checked = [
        MonthlyTripCount.model_validate(row).model_dump()
        for row in rows
    ]

    months = [
        row["month"]
        for row in checked
    ]

    if len(months) != len(set(months)):
        raise ValueError(
            "The extraction contains duplicate months."
        )

    return sorted(
        checked,
        key=lambda row: row["month"],
    )


def _make_metadata(
    rows: list[dict[str, Any]],
    extracted_at_utc: str,
) -> dict[str, Any]:
    """Describe the validated snapshot and its coverage."""

    months = [
        row["month"]
        for row in rows
    ]

    return {
        "source_table": SOURCE_TABLE,
        "metric": METRIC,
        "extraction_id": DOCUMENT_ID,
        "document_key": DOCUMENT_KEY,
        "schema_version": 1,
        "reporting_timezone": REPORTING_TIMEZONE,
        "available_months": months,

        # Retained for compatibility.
        # Use available_months when checking coverage.
        "timeframe": sorted({
            month[:4]
            for month in months
        }),

        "extracted_at_utc": extracted_at_utc,
    }


def generate_monthly_tripcount() -> dict[str, Any]:
    """Read one snapshot and always close its ClickHouse client."""

    client = get_clickhouse_client("citibike")

    try:
        result = client.query(MONTHLY_TRIPCOUNT_QUERY)
        raw_rows = list(result.named_results())

    finally:
        client.close()

    rows = _validate_rows(raw_rows)

    extracted_at_utc = datetime.now(
        timezone.utc
    ).isoformat()

    return {
        "rows": rows,
        "extraction_metadata": _make_metadata(
            rows,
            extracted_at_utc,
        ),
    }


def format_month(ym_str: str) -> str:
    """Convert 2026-04 to April 2026 (2026-04)."""

    value = datetime.strptime(
        ym_str,
        "%Y-%m",
    )

    return f"{value.strftime('%B %Y')} ({ym_str})"


def build_rag_document(
    data: dict[str, Any],
) -> dict[str, Any]:
    """Build a document using only the supplied extraction."""

    if not isinstance(data, dict):
        raise ValueError(
            "The extraction must be a dictionary."
        )

    rows = _validate_rows(data.get("rows"))
    metadata = data.get("extraction_metadata")

    if (
        not isinstance(metadata, dict)
        or metadata.get("source_table") != SOURCE_TABLE
        or metadata.get("metric") != METRIC
        or metadata.get("extraction_id") != DOCUMENT_ID
    ):
        raise ValueError(
            "The extraction metadata is incompatible."
        )

    extracted_at_utc = metadata.get(
        "extracted_at_utc"
    )

    if not isinstance(extracted_at_utc, str):
        raise ValueError(
            "The extraction timestamp is missing."
        )

    extracted_at = datetime.fromisoformat(
        extracted_at_utc
    )

    if extracted_at.utcoffset() != timedelta(0):
        raise ValueError(
            "The extraction timestamp must include UTC."
        )

    # Recompute coverage from validated rows.
    metadata = _make_metadata(
        rows,
        extracted_at_utc,
    )

    total_trips = sum(
        row["trip_count"]
        for row in rows
    )

    labels = ", ".join(
        format_month(row["month"])
        for row in rows
    )

    breakdown = "\n".join(
        f"- {format_month(row['month'])}: "
        f"{row['trip_count']:,} total trips"
        for row in rows
    )

    text = (
        "Citi Bike Monthly Trip Count Summary\n"
        f"Reporting timezone: {REPORTING_TIMEZONE}.\n"
        f"Available months: {labels}.\n"
        "Total trips across these available months: "
        f"{total_trips:,}.\n"
        "Only the listed months are represented "
        "in this summary.\n\n"
        f"Monthly Breakdown:\n{breakdown}"
    )

    return {
        "document_id": DOCUMENT_ID,
        "payload": {
            **metadata,
            "text": text,
            "rows": rows,
            "total_trips": total_trips,
        },
    }


def main() -> None:
    """Preview the document without calling Ollama or Qdrant."""

    data = generate_monthly_tripcount()
    document = build_rag_document(data)

    print(
        json.dumps(
            document,
            indent=2,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()