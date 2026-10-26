from dotenv import load_dotenv
import os

load_dotenv()


def get_clickhouse_creds():
    return {
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "HOST": "localhost",
        "PORT": os.getenv("CLICKHOUSE_HTTP_PORT"),
        "CLICKHOUSE_DB": os.getenv("CLICKHOUSE_DB"),
    }