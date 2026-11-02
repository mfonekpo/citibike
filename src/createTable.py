from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger
from uuid import uuid4


TABLE_NAME = "citibike.raw_citibike_tripdata"


TRIPDATA_V2_TABLE = "citibike.raw_citibike_tripdata_v2"


def create_tripdata_work_table(client: Client) -> str:
    work_table = f"citibike.tripdata_work_{uuid4().hex}"

    logger.info("Creating work table %s", work_table)

    client.command(
        f"CREATE TABLE {work_table} AS {TRIPDATA_V2_TABLE}"
    )

    return work_table

def create_tripdata_table(client: Client) -> None:
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


def create_stations_status_table(client: Client) -> None:
    TABLE_NAME = "citibike.stream_stations_status_raw"
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        (
            loaded_at DateTime64(3, 'UTC') DEFAULT now64(3),
            source_id LowCardinality(String) DEFAULT 'station_status',
            snapshot_id Nullable(UUID),
            raw_payload String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(loaded_at)
        ORDER BY loaded_at

    """)


def create_station_information_table(client: Client) -> None:
    TABLE_NAME = "citibike.stream_station_information_raw"
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        (
            loaded_at DateTime64(3, 'UTC') DEFAULT now64(3),
            source_id LowCardinality(String) DEFAULT 'station_information',
            raw_payload String            
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(loaded_at)
        ORDER BY loaded_at

    """)


def create_system_regions(client: Client) -> None:
    TABLE_NAME =    "citibike.stream_system_regions_raw"
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        (
            loaded_at DateTime64(3, 'UTC') DEFAULT now64(3),
            source_id LowCardinality(String) DEFAULT 'system_regions',
            raw_payload String            
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(loaded_at)
        ORDER BY loaded_at
    """)



def main():
    try:
        with get_clickhouse_client(db = "default") as client:
            logger.info("Creating tripdata_table")
            create_tripdata_table(client)

            logger.info("Creating station_status_table")
            create_stations_status_table(client)

            logger.info("Creating station_information_table")
            create_station_information_table(client)
            
            logger.info("Creating systems_regions_table")
            create_system_regions(client)
    except ClickHouseError:
        logger.exception("Failed to create tables.")
        raise


if __name__ == "__main__":
    main()