from uuid import NAMESPACE_URL, uuid5
from dotenv import load_dotenv
from utils.dwh_connect import get_clickhouse_client
from datetime import datetime
from typing import Any

load_dotenv()


def generate_monthly_tripcount()-> list[dict]:
    clickhouse_client = get_clickhouse_client("citibike")
    MONTHLY_TRIPCOUNT_QUERY = """
        SELECT
            formatDateTime(
                started_at, '%Y-%m', 'America/New_York'
            ) AS month,
            count() AS trip_count
        FROM fct_trips
        GROUP BY month
        ORDER BY month;
    """

    result = clickhouse_client.query(MONTHLY_TRIPCOUNT_QUERY)

    rows = list(result.named_results())
    return {
       "rows": rows,
       "extraction_metadata": {
           "source_table": "citibike.fct_trips",
           "extraction_id": str(uuid5(NAMESPACE_URL, MONTHLY_TRIPCOUNT_QUERY)),
           "metric": "monthly_trip_count",
           "timeframe": sorted({row["month"][:4] for row in rows}),
       }
    }


def format_month(ym_str: str) -> str:
  # Converts '2026-04' -> 'April 2026 (2026-04)'
  dt = datetime.strptime(ym_str, "%Y-%m")
  return f"{dt.strftime('%B %Y')} ({ym_str})"


def build_rag_document(data: list[dict[str, Any]]) -> dict[str, Any]:

    rows = data["rows"]
    metadata = data["extraction_metadata"]
    if not rows:
        return "No monthly trip count data available for Citibike."

    total_trips = sum(row["trip_count"] for row in rows)
    start_month = format_month(rows[0]["month"])
    end_month = format_month(rows[-1]["month"])

    lines = [
        f"- {format_month(row['month'])}: {row['trip_count']:,} total trips"
        for row in rows
    ]

    text = (
    "Citibike Monthly Trip Count Summary\n"
        f"Overview: Between {start_month} and {end_month}, Citibike recorded a "
        f"combined total of {total_trips:,} trips.\n\n"
        "Monthly Breakdown:\n" + "\n".join(lines)
    )
    return {
        "document_id": metadata["extraction_id"],
        "payload": {
            "text": text,
            "rows": rows,
            **metadata,
        }
    }


# def main():
#    data = generate_monthly_tripcount()
#    document = build_rag_document(data)
#    print(document["payload"]["text"])


# if __name__ == "__main__":
#     main()