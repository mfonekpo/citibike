from dotenv import load_dotenv
import os

load_dotenv()


def get_clickhouse_creds():
    return {
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "HOST": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "PORT": os.getenv("CLICKHOUSE_HTTP_PORT"),
        "CLICKHOUSE_DB": os.getenv("CLICKHOUSE_DB"),
    }


def get_baserow_creds() -> dict:
    token = os.getenv("BASEROW_API_TOKEN", "").strip()

    if not token:
        raise ValueError("BASEROW_API_TOKEN is missing.")

    try:
        table_id = int(os.getenv("BASEROW_TABLE_ID", ""))
    except ValueError:
        raise ValueError(
            "BASEROW_TABLE_ID must be an integer."
        ) from None

    if not 0 < table_id < 2**64:
        raise ValueError("BASEROW_TABLE_ID is outside the valid range.")

    return {
        "table_id": table_id,
        "token": token,
    }