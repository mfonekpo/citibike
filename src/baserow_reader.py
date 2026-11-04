from datetime import UTC, datetime
from uuid import uuid4

import requests

from utils.credentials import get_baserow_creds
from utils.logging_config import logger


def fetch_baserow_tickets(
    *, table_id: int, token: str, page_size: int = 200
) -> dict:
    if not 1 <= page_size <= 200:
        raise ValueError("page_size must be between 1 and 200.")

    url = f"https://api.baserow.io/api/database/rows/table/{table_id}/"
    extraction_id = uuid4()
    extracted_at = datetime.now(UTC)

    records = []
    seen_ids = set()
    expected_count = None

    with requests.Session() as session:
        session.headers.update({"Authorization": f"Token {token}"})

        for page in range(1, 1001):
            response = session.get(
                url,
                params={
                    "user_field_names": "true",
                    "size": page_size,
                    "page": page,
                },
                timeout=(10, 30),
                allow_redirects=False,
            )

            response.raise_for_status()

            if response.status_code != 200:
                raise ValueError(
                    f"Unexpected HTTP status: {response.status_code}"
                )

            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Expected a JSON object.")

            count = payload.get("count")
            page_rows = payload.get("results")

            if type(count) is not int or count < 0:
                raise ValueError("Missing or invalid response count.")

            if not isinstance(page_rows, list) or "next" not in payload:
                raise ValueError("Missing or invalid pagination fields.")

            if expected_count is None:
                expected_count = count
            elif count != expected_count:
                raise ValueError(
                    "Row count changed during extraction. Run again."
                )

            for row in page_rows:
                if not isinstance(row, dict):
                    raise ValueError(
                        "Expected each ticket to be a JSON object."
                    )

                row_id = row.get("id")

                if type(row_id) is not int or not 0 < row_id < 2**64:
                    raise ValueError(
                        "Ticket has an invalid Baserow row ID."
                    )

                if row_id in seen_ids:
                    raise ValueError(
                        f"Duplicate Baserow row ID: {row_id}"
                    )

                seen_ids.add(row_id)
                records.append(row)

            logger.info(
                "Fetched Baserow page %s: %s rows",
                page,
                len(page_rows),
            )

            if payload["next"] is None:
                if len(records) != expected_count:
                    raise ValueError(
                        f"Expected {expected_count} rows; "
                        f"received {len(records)}."
                    )

                return {
                    "baserow_table_id": table_id,
                    "extraction_id": extraction_id,
                    "extracted_at": extracted_at,
                    "rows": records,
                }

            if not page_rows or len(records) >= expected_count:
                raise ValueError(
                    "Pagination disagrees with the reported row count."
                )

    raise ValueError("Exceeded the limit of 1,000 pages.")


def main() -> None:
    try:
        credentials = get_baserow_creds()

        extraction = fetch_baserow_tickets(
            table_id=credentials["table_id"],
            token=credentials["token"],
        )

        logger.info(
            "Verified Baserow extraction %s: %s rows from table %s. "
            "No rows inserted.",
            extraction["extraction_id"],
            len(extraction["rows"]),
            extraction["baserow_table_id"],
        )

    except (requests.RequestException, ValueError):
        logger.exception("Baserow extraction failed.")
        raise


if __name__ == "__main__":
    main()