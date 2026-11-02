from collections.abc import Iterable
from src.createTable import create_tripdata_table
from contextlib import closing
from utils.s3_connect import get_minio_client, required_env
from src.readObject import get_object_keys

import pandas as pd
from clickhouse_connect.driver.client import Client

from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger


TABLE_NAME = "citibike.raw_citibike_tripdata"
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


def ingest_data(
    client: Client,
    minio_client,
    bucket: str,
    object_keys: Iterable[str],
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


    for data_keys in object_keys:
        source_locator = f"s3://{bucket}/{data_keys}"
        logger.info("Ingesting %s", source_locator)

        obj = minio_client.get_object(
            Bucket=bucket,
            Key=data_keys,
        )
        with closing(obj["Body"]) as body, pd.read_csv(
            body,
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
                        f"Unexpected columns in {data_keys}: "
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
                    data_keys,
                    f"{total_rows:,}",
                )

    return total_rows


def data_loader(prefix: str = "") -> int:
    with closing(get_minio_client()) as minio_client:
        bucket = required_env("MINIO_BUCKET")

        object_keys = get_object_keys(
            minio_client,
            bucket,
            prefix=prefix,
        )

        if not object_keys:
            raise FileNotFoundError(
                f"No CSV objects found in s3://{bucket}/{prefix}"
            )

        with get_clickhouse_client(db="default") as client:
            create_tripdata_table(client)

            return ingest_data(
                client=client,
                minio_client=minio_client,
                bucket=bucket,
                object_keys=object_keys,
            )


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
