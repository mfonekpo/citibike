from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from utils.dwh_connect import get_clickhouse_client
from utils.logging_config import logger


def create_stations_status_table(client: Client) -> None:
    TABLE_NAME = "citibike.stream_stations_status_raw"
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        (
            loaded_at DateTime64(3, 'UTC') DEFAULT now64(3),
            source_id LowCardinality(String) DEFAULT 'station_status',
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
            source_id LowCardinality(String) DEFAULT 'station_status',
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
        with get_clickhouse_client(db = "citibike") as client:
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