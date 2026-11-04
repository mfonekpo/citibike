"""Run baseline SQL on isolated ClickHouse fixtures, without a warehouse connection.

Requires Jinja2 and the ``clickhouse`` binary. Run directly with Python or with
``python -m unittest discover -s tests -p 'test_gold_baseline.py'``.
Three scenarios exercise 15 expected values and the two dbt tests per scenario:
21 result checks in total. The SQL is rendered from the project's model files.
"""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from jinja2 import Environment, StrictUndefined


DBT_PROJECT = Path(__file__).resolve().parents[1] / "citibike_analytics"
CLICKHOUSE = shutil.which("clickhouse")


@unittest.skipUnless(CLICKHOUSE, "The clickhouse binary is required for SQL fixtures")
class HistoricalDemandBaselineTests(unittest.TestCase):
    def check_scenario(self, start_date, end_date, inserts, expectations):
        variables = {
            "analytics_timezone": "America/New_York",
            "trip_reporting_start_date": start_date,
            "trip_reporting_end_date": end_date,
        }
        environment = Environment(undefined=StrictUndefined)

        def render(path):
            return environment.from_string(path.read_text()).render(
                config=lambda **_: "",
                ref=lambda name: name,
                var=lambda name, default=None: variables.get(name, default),
            )

        setup_sql = """
CREATE TABLE dim_stations (station_id Nullable(String)) ENGINE=Memory;
INSERT INTO dim_stations VALUES ('active'), ('outside_only'), ('never_observed');
CREATE TABLE agg_station_hourly_usage (
    station_id String,
    hour_start_utc DateTime('UTC'),
    departures UInt64,
    arrivals UInt64
) ENGINE=Memory;
"""
        baseline_sql = render(
            DBT_PROJECT / "models/intermediate/int_station_hourly_demand_baseline.sql"
        )
        sql = (
            setup_sql
            + inserts
            + ";\nCREATE TABLE int_station_hourly_demand_baseline ENGINE=Memory AS "
            + baseline_sql
            + ";\n"
        )
        checks = [(query, expected, query) for query, expected in expectations]
        for test_path in sorted((DBT_PROJECT / "tests").glob("baseline_*.sql")):
            checks.append(
                (f"(SELECT count() FROM ({render(test_path)}))", 0, test_path.name)
            )
        sql += "\n".join(
            f"SELECT {query} AS actual FORMAT JSONEachRow;"
            for query, _, _ in checks
        )

        with tempfile.TemporaryDirectory(prefix="citibike-baseline-fixture-") as local_path:
            result = subprocess.run(
                [CLICKHOUSE, "local", "--path", local_path, "--multiquery"],
                input=sql,
                text=True,
                capture_output=True,
                timeout=120,
                check=False,
            )
        if result.returncode:
            self.fail(f"ClickHouse fixture failed:\n{result.stderr}")
        actual_values = [json.loads(line)["actual"] for line in result.stdout.splitlines()]
        if len(actual_values) != len(checks):
            self.fail(f"Expected {len(checks)} result rows; got {actual_values!r}")
        for actual, (_, expected, label) in zip(actual_values, checks):
            with self.subTest(check=label):
                self.assertEqual(actual, expected)

    def test_reporting_bounds_zero_trip_dates_and_unknown_history(self):
        self.check_scenario(
            "2026-09-01",
            "2026-10-01",
            """
INSERT INTO agg_station_hourly_usage VALUES
('active', '2026-09-01 13:00:00', 8, 4),
('active', '2026-09-08 13:00:00', 2, 6),
('active', '2026-09-01 03:00:00', 100, 100),
('active', '2026-10-01 04:00:00', 200, 200),
('outside_only', '2026-08-25 13:00:00', 10, 10)
""",
            [
                ("(SELECT count() FROM int_station_hourly_demand_baseline)", 504),
                ("(SELECT baseline_days FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=2 AND local_hour=9)", 5),
                ("(SELECT avg_departures FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=2 AND local_hour=9)", 2),
                ("(SELECT avg_arrivals FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=2 AND local_hour=9)", 2),
                ("(SELECT avg_departures FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=2 AND local_hour=10)", 0),
                ("(SELECT sum(total_departures) FROM int_station_hourly_demand_baseline)", 10),
                ("(SELECT count() FROM int_station_hourly_demand_baseline WHERE station_id='outside_only' AND station_in_reporting_data=0 AND avg_departures IS NULL)", 168),
                ("(SELECT count() FROM int_station_hourly_demand_baseline WHERE station_id='never_observed' AND avg_arrivals IS NULL)", 168),
            ],
        )

    def test_fall_back_has_two_hour_opportunities_on_one_local_date(self):
        self.check_scenario(
            "2026-11-01",
            "2026-11-02",
            """
INSERT INTO agg_station_hourly_usage VALUES
('active', '2026-11-01 05:00:00', 4, 2),
('active', '2026-11-01 06:00:00', 6, 4)
""",
            [
                ("(SELECT baseline_days FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=7 AND local_hour=1)", 1),
                ("(SELECT baseline_hour_opportunities FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=7 AND local_hour=1)", 2),
                ("(SELECT avg_departures FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=7 AND local_hour=1)", 5),
                ("(SELECT sum(baseline_hour_opportunities) FROM int_station_hourly_demand_baseline WHERE station_id='active')", 25),
            ],
        )

    def test_spring_forward_missing_hour_is_unknown_not_zero(self):
        self.check_scenario(
            "2026-03-08",
            "2026-03-09",
            """
INSERT INTO agg_station_hourly_usage VALUES
('active', '2026-03-08 07:00:00', 4, 2)
""",
            [
                ("(SELECT baseline_hour_opportunities FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=7 AND local_hour=2)", 0),
                ("(SELECT avg_departures FROM int_station_hourly_demand_baseline WHERE station_id='active' AND local_weekday=7 AND local_hour=2)", None),
                ("(SELECT sum(baseline_hour_opportunities) FROM int_station_hourly_demand_baseline WHERE station_id='active')", 23),
            ],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
