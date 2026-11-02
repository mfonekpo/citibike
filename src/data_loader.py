from collections.abc import Iterable
from src.createTable import create_tripdata_table
from contextlib import closing
from utils.s3_connect import get_minio_client, required_env
from src.manifest_reader import read_batch_manifest, verify_manifest_objects
import argparse
from datetime import date
from src.createTable import (
    TRIPDATA_V2_TABLE,
    create_tripdata_work_table,
)

import pandas as pd
from clickhouse_connect.driver.client import Client
from botocore.exceptions import BotoCoreError, ClientError
from clickhouse_connect.driver.exceptions import ClickHouseError

from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger


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
    verified_files: Iterable[dict],
    chunk_size: int = 100_000,
    *,
    table_name: str,
    batch_id: str,
    release_month: date,
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


    for file_info in verified_files:
        data_keys = file_info["key"]
        file_rows = 0
        source_locator = f"s3://{bucket}/{data_keys}"
        logger.info("Ingesting %s", source_locator)

        obj = minio_client.get_object(
            Bucket=bucket,
            Key=data_keys,
            IfMatch=file_info["etag"],
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
                chunk["batch_id"] = batch_id
                chunk["release_month"] = release_month

                if insert_context is None:
                    insert_context = client.create_insert_context(
                        table=table_name,
                        column_names=[
                            *SOURCE_COLUMNS,
                            "source_locator",
                            "batch_id",
                            "release_month",
                        ],
                        settings={"async_insert": 0},
                    )

                client.insert_df(df=chunk, context=insert_context)
                total_rows += len(chunk)
                file_rows += len(chunk)
                logger.info(
                    "Inserted %s rows from %s; %s rows total.",
                    f"{len(chunk):,}",
                    data_keys,
                    f"{total_rows:,}",
                )
        if file_rows != file_info["row_count"]:
            raise ValueError(
                f"Row count mismatch for {data_keys}: "
                f"expected {file_info['row_count']}, got {file_rows}."
            )

    return total_rows


def verify_loaded_batch(
    client: Client,
    work_table: str,
    bucket: str,
    files: list[dict],
    batch_id: str,
    release_month: date,
) -> None:
    expected = {
        f"s3://{bucket}/{item['key']}": item["row_count"]
        for item in files
        if item["row_count"] > 0
    }

    result = client.query(
        f"""
        SELECT
            source_locator,
            batch_id,
            release_month,
            count() AS row_count
        FROM {work_table}
        GROUP BY source_locator, batch_id, release_month
        """,
        settings={"use_query_cache": 0},
    )

    actual = {}

    for locator, stored_batch, stored_month, rows in result.result_rows:
        if stored_batch != batch_id or stored_month != release_month:
            raise ValueError(
                "Work table contains unexpected batch metadata."
            )

        actual[locator] = rows

    if actual != expected:
        raise ValueError(
            f"Stored row counts differ: "
            f"expected={expected}, actual={actual}"
        )

    logger.info("Verified ClickHouse row counts for %s", work_table)


def publish_tripdata_month(
    client: Client,
    work_table: str,
    release_month: date,
) -> None:
    partition_id = release_month.year * 100 + release_month.month

    client.command(
        f"ALTER TABLE {TRIPDATA_V2_TABLE} "
        f"REPLACE PARTITION {partition_id} FROM {work_table}"
    )

    logger.info(
        "Published partition %s to %s",
        partition_id,
        TRIPDATA_V2_TABLE,
    )


def data_loader(
    manifest_key: str,
    data_prefix: str = "",
    validate_only: bool = False,
) -> int:
    with closing(get_minio_client()) as minio_client:
        bucket = required_env("MINIO_BUCKET")

        manifest = read_batch_manifest(
            minio_client, bucket, manifest_key
        )
        files = verify_manifest_objects(
            minio_client, bucket, manifest, data_prefix
        )

        logger.info(
            "Verified batch %s: %s files, %s expected rows.",
            manifest["batch_id"],
            len(files),
            manifest["total_rows"],
        )

        if validate_only:
            logger.info("Validation only: no rows inserted.")
            return 0

        if manifest["total_rows"] <= 0:
            raise ValueError(
                "This publication flow requires a non-empty release."
            )

        release_month = date.fromisoformat(
            f'{manifest["release_month"]}-01'
        )

        with get_clickhouse_client(db="default") as client:
            work_table = create_tripdata_work_table(client)

            try:
                total_rows = ingest_data(
                    client=client,
                    minio_client=minio_client,
                    bucket=bucket,
                    verified_files=files,
                    table_name=work_table,
                    batch_id=manifest["batch_id"],
                    release_month=release_month,
                )

                if total_rows != manifest["total_rows"]:
                    raise ValueError(
                        "Loaded total differs from the manifest."
                    )

                verify_loaded_batch(
                    client,
                    work_table,
                    bucket,
                    files,
                    manifest["batch_id"],
                    release_month,
                )

                publish_tripdata_month(
                    client, work_table, release_month
                )

            except (ValueError, BotoCoreError, ClientError, ClickHouseError):
                logger.exception(
                    "Load or publication failed; "
                    "work table retained: %s",
                    work_table,
                )
                raise

            try:
                client.command(f"DROP TABLE {work_table}")
            except ClickHouseError:
                logger.warning(
                    "Publication succeeded, but cleanup failed for %s",
                    work_table,
                    exc_info=True,
                )

            return total_rows

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--data-prefix", default="")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    try:
        total_rows = data_loader(
            manifest_key=args.manifest_key,
            data_prefix=args.data_prefix,
            validate_only=args.validate_only,
        )
    except Exception:
        logger.exception("Citi Bike ingestion failed.")
        raise

    if not args.validate_only:
        logger.info(
            "Citi Bike ingestion completed: %s rows inserted.",
            total_rows,
        )


if __name__ == "__main__":
    main()