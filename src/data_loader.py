from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from clickhouse_connect.driver.client import Client

from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger


PROJECT_DIR = Path(__file__).resolve().parents[1]
CSV_DIR = PROJECT_DIR / "202609-citibike-tripdata"
TABLE_NAME = "citibike.raw_citibike"
SOURCE_COLUMNS = (
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
)


def create_table(client: Client) -> None:
    """Create the raw database and table using a caller-owned client."""
    client.command("CREATE DATABASE IF NOT EXISTS citibike")
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        (
            ride_id String,
            rideable_type String,
            started_at String,
            ended_at String,
            start_station_name String,
            start_station_id String,
            end_station_name String,
            end_station_id String,
            start_lat String,
            start_lng String,
            end_lat String,
            end_lng String,
            member_casual String,
            source_locator String,
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        ORDER BY ride_id
    """)


def ingest_data(
    client: Client,
    csv_paths: Iterable[Path],
    chunk_size: int = 100_000,
) -> int:
    """Append raw CSV rows to the existing table and return the inserted count.

    The caller owns the client. Inserts are sequential; this function does not
    retry failed inserts or deduplicate rows from previous runs.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    total_rows = 0
    insert_context = None
    expected_columns = set(SOURCE_COLUMNS)

    for csv_path in csv_paths:
        csv_path = Path(csv_path).resolve()
        source_locator = (
            str(csv_path.relative_to(PROJECT_DIR))
            if csv_path.is_relative_to(PROJECT_DIR)
            else str(csv_path)
        )

        with pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
            chunksize=chunk_size,
        ) as reader:
            for chunk in reader:
                actual_columns = set(chunk.columns)
                if (
                    actual_columns != expected_columns
                    or len(chunk.columns) != len(SOURCE_COLUMNS)
                ):
                    missing = sorted(expected_columns - actual_columns)
                    unexpected = sorted(actual_columns - expected_columns)
                    raise ValueError(
                        f"Unexpected columns in {csv_path.name}: "
                        f"missing={missing}, unexpected={unexpected}."
                    )

                if chunk.empty:
                    continue

                # InsertContext maps values by position, so use a fixed order.
                chunk = chunk.loc[:, list(SOURCE_COLUMNS)].copy()
                chunk["source_locator"] = source_locator

                if insert_context is None:
                    # Reuse the table metadata across all sequential chunks.
                    # Omit ingested_at so ClickHouse supplies its default.
                    insert_context = client.create_insert_context(
                        table=TABLE_NAME,
                        column_names=[*SOURCE_COLUMNS, "source_locator"],
                    )

                client.insert_df(df=chunk, context=insert_context)
                total_rows += len(chunk)
                logger.info(
                    "Inserted %s rows from %s; %s rows total.",
                    f"{len(chunk):,}",
                    csv_path.name,
                    f"{total_rows:,}",
                )

    return total_rows


def data_loader(data_dir: Path = CSV_DIR) -> int:
    """Create the raw table, then load each CSV using one managed client."""
    csv_paths = sorted(path for path in Path(data_dir).glob("*.csv") if path.is_file())
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in {data_dir}.")

    # Connect to an existing database so citibike can be created on a fresh run.
    with get_clickhouse_client(db="default") as client:
        create_table(client)
        return ingest_data(client, csv_paths)


def main() -> None:
    """Run the pipeline with progress and failure details in the shared logs."""
    logger.info("Starting Citi Bike ingestion.")
    try:
        total_rows = data_loader()
    except Exception:
        logger.exception("Citi Bike ingestion failed.")
        raise
    logger.info("Citi Bike ingestion completed: %s rows inserted.", f"{total_rows:,}")


if __name__ == "__main__":
    main()
