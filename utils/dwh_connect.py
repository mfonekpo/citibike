import clickhouse_connect
from clickhouse_connect.driver.client import Client

from utils.credentials import get_clickhouse_creds


def get_clickhouse_client(db: str = "default") -> Client:
    """Return a client for the caller to close, preferably with a with block.

    Connection errors propagate so callers and Airflow can detect failed runs.
    """
    creds = get_clickhouse_creds()
    return clickhouse_connect.get_client(
        host=creds["HOST"],
        port=int(creds["PORT"] or 8123),
        username=creds["CLICKHOUSE_USER"],
        password=creds["CLICKHOUSE_PASSWORD"],
        database=db,
        secure=False,
        connect_timeout=10,
    )
