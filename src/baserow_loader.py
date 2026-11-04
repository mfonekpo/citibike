import json
from datetime import UTC

from clickhouse_connect.driver.client import Client

from src.baserow_reader import fetch_baserow_tickets
from utils.credentials import get_baserow_creds
from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger


RAW_TABLE = "citibike.baserow_maintenance_tickets_raw"
COMPLETED_TABLE = "citibike.baserow_completed_extractions"


def load_baserow_extraction(client: Client, extraction: dict) -> int:
    table_id = extraction["baserow_table_id"]
    extraction_id = extraction["extraction_id"]
    extracted_at = extraction["extracted_at"]
    records = extraction["rows"]
    expected_rows = len(records)

    if extracted_at.tzinfo is None or extracted_at.utcoffset() is None:
        raise ValueError("extracted_at must include a timezone.")

    extracted_at = extracted_at.astimezone(UTC)

    # Prepare all records before inserting anything.
    raw_rows = []
    seen_ids = set()

    for record in records:
        row_id = record["id"]

        if row_id in seen_ids:
            raise ValueError(f"Duplicate Baserow row ID: {row_id}")

        seen_ids.add(row_id)

        raw_rows.append((
            table_id,
            row_id,
            extraction_id,
            extracted_at,
            json.dumps(record, ensure_ascii=False, allow_nan=False),
        ))

    parameters = {
        "table_id": table_id,
        "extraction_id": str(extraction_id),
    }

    scope = """
        WHERE baserow_table_id = {table_id:UInt64}
          AND extraction_id = {extraction_id:UUID}
    """

    # Reject accidental reuse of an extraction already in ClickHouse.
    for table in (RAW_TABLE, COMPLETED_TABLE):
        existing_rows = client.query(
            f"SELECT count() FROM {table} {scope}",
            parameters=parameters,
            settings={"use_query_cache": 0},
        ).first_row[0]

        if existing_rows:
            raise ValueError(
                f"Extraction {extraction_id} already exists in {table}. "
                "Run the reader again to start a new extraction."
            )

    if raw_rows:
        client.insert(
            table=RAW_TABLE,
            data=raw_rows,
            column_names=[
                "baserow_table_id",
                "baserow_row_id",
                "extraction_id",
                "extracted_at",
                "raw_payload",
            ],
            settings={"async_insert": 0},
        )

    actual_rows, distinct_ids = client.query(
        f"""
        SELECT count(), uniqExact(baserow_row_id)
        FROM {RAW_TABLE}
        {scope}
        """,
        parameters=parameters,
        settings={"use_query_cache": 0},
    ).first_row

    if actual_rows != expected_rows or distinct_ids != expected_rows:
        raise ValueError(
            f"Verification failed for {extraction_id}: "
            f"expected {expected_rows}, found {actual_rows} rows "
            f"and {distinct_ids} distinct IDs."
        )

    logger.info(
        "Verified extraction %s: %s rows and %s distinct IDs",
        extraction_id,
        actual_rows,
        distinct_ids,
    )

    # Record completion only after verification succeeds.
    client.insert(
        table=COMPLETED_TABLE,
        data=[(table_id, extraction_id, extracted_at, expected_rows)],
        column_names=[
            "baserow_table_id",
            "extraction_id",
            "extracted_at",
            "expected_rows",
        ],
        settings={"async_insert": 0},
    )

    return expected_rows



def main() -> None:
    extraction = None

    try:
        credentials = get_baserow_creds()

        extraction = fetch_baserow_tickets(
            table_id=credentials["table_id"],
            token=credentials["token"],
        )

        logger.info(
            "Loading Baserow extraction %s: %s tickets",
            extraction["extraction_id"],
            len(extraction["rows"]),
        )

        with get_clickhouse_client(db="citibike") as client:
            loaded_rows = load_baserow_extraction(
                client,
                extraction,
            )

        logger.info(
            "Completed Baserow extraction %s: %s tickets",
            extraction["extraction_id"],
            loaded_rows,
        )

    except Exception:
        extraction_id = (
            extraction["extraction_id"]
            if extraction is not None
            else "unavailable"
        )

        logger.exception(
            "Baserow load did not finish cleanly; extraction_id=%s",
            extraction_id,
        )
        raise


if __name__ == "__main__":
    main()