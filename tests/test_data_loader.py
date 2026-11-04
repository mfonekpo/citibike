"""Regression tests using real CSV readers and SDK contexts, without a server."""

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import create_autospec, patch

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.insert import InsertContext

from src import data_loader as loader
from utils.dwh_connect import get_clickhouse_client


CSV_COLUMNS = (
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name",
    "end_station_id", "start_lat", "start_lng", "end_lat",
    "end_lng", "member_casual",
)


class LoaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.project_dir = Path(directory.name)
        self.data_dir = self.project_dir / "trips"
        self.data_dir.mkdir()
        project_patch = patch.object(loader, "PROJECT_DIR", self.project_dir)
        project_patch.start()
        self.addCleanup(project_patch.stop)

        self.client = create_autospec(Client, instance=True)
        self.client.__enter__.return_value = self.client
        self.client.__exit__.side_effect = self.close_client
        self.client.create_insert_context.side_effect = self.make_context
        self.client.insert_df.side_effect = self.record_insert
        self.inserted_rows = []

    def close_client(self, *_):
        self.client.close()
        return False

    @staticmethod
    def make_context(table, column_names):
        return InsertContext(
            table=table,
            column_names=column_names,
            column_types=[get_from_name("String") for _ in column_names],
        )

    def record_insert(self, *, df, context):
        # Exercise the installed SDK's actual positional DataFrame conversion.
        self.assertTrue(context.empty)
        context.data = df
        for block in context.next_block():
            for values in zip(*block.column_data):
                self.inserted_rows.append(dict(zip(block.column_names, values)))
        # Successful HTTP inserts clear the context so it can be reused.
        context.data = None

    def write_csv(self, name, ride_ids, columns=CSV_COLUMNS):
        path = self.data_dir / name
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            for ride_id in ride_ids:
                row = dict.fromkeys(CSV_COLUMNS, "")
                row.update(
                    ride_id=ride_id,
                    rideable_type="classic_bike",
                    started_at="2026-09-01 10:00:00.123",
                    ended_at="2026-09-01 10:05:00.123",
                    start_station_id="5980.10",
                    end_station_id="0012",
                    start_lat="40.700",
                    member_casual="member",
                )
                writer.writerow(row.get(column, "extra") for column in columns)
        return path

    def test_create_table_only_runs_ddl(self):
        loader.create_table(self.client)

        statements = [call.args[0] for call in self.client.command.call_args_list]
        self.assertEqual(statements[0], "CREATE DATABASE IF NOT EXISTS citibike")
        self.assertEqual(len(statements), 2)
        self.assertIn("CREATE TABLE IF NOT EXISTS citibike.raw_citibike_tripdata", statements[1])
        self.assertIn("DEFAULT now64(3)", statements[1])
        self.client.insert_df.assert_not_called()
        self.client.create_insert_context.assert_not_called()
        self.client.close.assert_not_called()

    def test_multiple_files_and_chunks_preserve_raw_values_and_column_order(self):
        first = self.write_csv("a.csv", ["ride-1", "ride-2"])
        second = self.write_csv("b.csv", ["ride-3"], columns=CSV_COLUMNS[::-1])

        count = loader.ingest_data(self.client, [first, second], chunk_size=1)

        self.assertEqual(count, 3)
        self.assertEqual([row["ride_id"] for row in self.inserted_rows],
                         ["ride-1", "ride-2", "ride-3"])
        self.assertEqual([row["source_locator"] for row in self.inserted_rows],
                         ["trips/a.csv", "trips/a.csv", "trips/b.csv"])
        for row in self.inserted_rows:
            self.assertEqual(row["start_station_id"], "5980.10")
            self.assertEqual(row["end_station_id"], "0012")
            self.assertEqual(row["start_lat"], "40.700")
            self.assertEqual(row["end_station_name"], "")
            self.assertEqual(row["started_at"], "2026-09-01 10:00:00.123")
            self.assertNotIn("ingested_at", row)
        self.client.create_insert_context.assert_called_once_with(
            table="citibike.raw_citibike_tripdata",
            column_names=[*CSV_COLUMNS, "source_locator"],
        )
        contexts = [call.kwargs["context"]
                    for call in self.client.insert_df.call_args_list]
        self.assertTrue(all(context is contexts[0] for context in contexts))
        self.client.command.assert_not_called()
        self.client.close.assert_not_called()

    def test_invalid_headers_fail_before_inserting(self):
        headers = (
            CSV_COLUMNS[:-1],
            (*CSV_COLUMNS, "unexpected"),
            (*CSV_COLUMNS[:-1], "ride_id"),
        )
        for index, columns in enumerate(headers):
            with self.subTest(columns=columns):
                path = self.write_csv(f"bad-{index}.csv", ["ride-1"], columns)
                with self.assertRaisesRegex(ValueError, "Unexpected columns"):
                    loader.ingest_data(self.client, [path])
        self.client.insert_df.assert_not_called()
        self.client.create_insert_context.assert_not_called()

    def test_header_only_csv_does_not_insert(self):
        path = self.write_csv("empty.csv", [])

        self.assertEqual(loader.ingest_data(self.client, [path]), 0)
        self.client.insert_df.assert_not_called()
        self.client.create_insert_context.assert_not_called()

    def test_invalid_chunk_size_fails_before_reading(self):
        for chunk_size in (0, -1):
            with self.subTest(chunk_size=chunk_size):
                with self.assertRaisesRegex(ValueError, "chunk_size"):
                    loader.ingest_data(self.client, [], chunk_size=chunk_size)

    def test_missing_files_fail_before_connecting(self):
        with patch.object(loader, "get_clickhouse_client") as connect:
            with self.assertRaisesRegex(FileNotFoundError, "No CSV files"):
                loader.data_loader(self.data_dir)
        connect.assert_not_called()

    def test_loader_connects_once_bootstraps_then_loads_sorted_files(self):
        self.write_csv("b.csv", ["ride-b"])
        self.write_csv("a.csv", ["ride-a"])
        (self.data_dir / "ignore.txt").write_text("ignore", encoding="utf-8")
        (self.data_dir / "directory.csv").mkdir()

        with patch.object(loader, "get_clickhouse_client",
                          return_value=self.client) as connect:
            count = loader.data_loader(self.data_dir)

        self.assertEqual(count, 2)
        self.assertEqual([row["ride_id"] for row in self.inserted_rows],
                         ["ride-a", "ride-b"])
        connect.assert_called_once_with(db="default")
        self.assertEqual(self.client.command.call_count, 2)
        self.client.close.assert_called_once()

    def test_insert_failure_propagates_and_closes_reader_and_client(self):
        path = self.write_csv("a.csv", ["ride-1", "ride-2"])
        reader = loader.pd.read_csv(
            path, dtype=str, keep_default_na=False, encoding="utf-8-sig",
            chunksize=1,
        )
        self.addCleanup(reader.close)
        failure = OperationalError("simulated insert failure")

        def fail_second_insert(**kwargs):
            if self.inserted_rows:
                raise failure
            self.record_insert(**kwargs)

        self.client.insert_df.side_effect = fail_second_insert
        with (
            patch.object(loader, "get_clickhouse_client", return_value=self.client),
            patch.object(loader.pd, "read_csv", return_value=reader),
            patch.object(reader, "close", wraps=reader.close) as close_reader,
        ):
            with self.assertRaises(OperationalError) as raised:
                loader.data_loader(self.data_dir)

        self.assertIs(raised.exception, failure)
        self.assertEqual(self.client.insert_df.call_count, 2)
        self.assertEqual(len(self.inserted_rows), 1)
        close_reader.assert_called_once()
        self.client.close.assert_called_once()

    def test_ddl_failure_propagates_and_closes_client(self):
        self.write_csv("a.csv", ["ride-1"])
        failure = OperationalError("simulated DDL failure")
        self.client.command.side_effect = failure

        with patch.object(loader, "get_clickhouse_client", return_value=self.client):
            with self.assertRaises(OperationalError) as raised:
                loader.data_loader(self.data_dir)

        self.assertIs(raised.exception, failure)
        self.client.insert_df.assert_not_called()
        self.client.close.assert_called_once()


class ClientFactoryTests(unittest.TestCase):
    CREDS = {
        "HOST": "localhost",
        "PORT": "8123",
        "CLICKHOUSE_USER": "test-user",
        "CLICKHOUSE_PASSWORD": "test-password",
    }

    def test_factory_uses_keyword_arguments_and_integer_port(self):
        with (
            patch("utils.dwh_connect.get_clickhouse_creds", return_value=self.CREDS),
            patch("utils.dwh_connect.clickhouse_connect.get_client") as connect,
        ):
            client = get_clickhouse_client(db="citibike")

        self.assertIs(client, connect.return_value)
        connect.assert_called_once_with(
            host="localhost", port=8123, username="test-user",
            password="test-password", database="citibike", secure=False,
            connect_timeout=10,
        )

    def test_factory_defaults_to_existing_database_and_http_port(self):
        with (
            patch("utils.dwh_connect.get_clickhouse_creds",
                  return_value={**self.CREDS, "PORT": None}),
            patch("utils.dwh_connect.clickhouse_connect.get_client") as connect,
        ):
            get_clickhouse_client()

        self.assertEqual(connect.call_args.kwargs["database"], "default")
        self.assertEqual(connect.call_args.kwargs["port"], 8123)

    def test_factory_propagates_connection_failure(self):
        failure = OperationalError("simulated connection failure")
        with (
            patch("utils.dwh_connect.get_clickhouse_creds", return_value=self.CREDS),
            patch("utils.dwh_connect.clickhouse_connect.get_client",
                  side_effect=failure),
        ):
            with self.assertRaises(OperationalError) as raised:
                get_clickhouse_client()

        self.assertIs(raised.exception, failure)


if __name__ == "__main__":
    unittest.main()
