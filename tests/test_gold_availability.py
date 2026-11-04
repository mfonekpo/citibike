"""Offline behavioral checks: synthetic data, actual model SQL, no warehouse access."""
from pathlib import Path
import subprocess
import tempfile
import shutil
import unittest
from jinja2 import Environment, StrictUndefined

root = Path(__file__).resolve().parents[1] / 'citibike_analytics'
env = Environment(undefined=StrictUndefined)
env.globals.update(config=lambda **kw: '', ref=lambda name: name, var=lambda name, default=None: default)
model = env.from_string((root/'models/marts/agg_station_hourly_availability.sql').read_text()).render()
columns=['source_id','station_id','feed_updated_at','last_reported_at','loaded_at','snapshot_id','num_bikes_available','num_ebikes_available','num_bikes_disabled','num_docks_available','num_docks_disabled','is_installed','is_renting','is_returning']
def row(station, **kwargs):
    values=dict(source_id='station_status',station_id=station,feed_updated_at='2026-10-30 12:00:00',last_reported_at='2026-10-30 12:00:00',loaded_at='2026-10-30 12:00:05',snapshot_id='a',num_bikes_available=5,num_ebikes_available=2,num_bikes_disabled=0,num_docks_available=8,num_docks_disabled=0,is_installed=1,is_renting=1,is_returning=1)
    values.update(kwargs)
    return '('+','.join('NULL' if values[c] is None else "'"+values[c]+"'" if isinstance(values[c], str) else str(values[c]) for c in columns)+')'
rows=[
 row('repeat'),row('repeat',loaded_at='2026-10-30 12:10:00',snapshot_id='b'),
 row('conflict'),row('conflict',num_bikes_available=0,loaded_at='2026-10-30 12:00:10',snapshot_id='b'),
 row('null_conflict',num_bikes_available=None),row('null_conflict',num_bikes_available=0,snapshot_id='b'),
 row('different_sources'),row('different_sources',source_id='other',num_bikes_available=7),
 row('stale',loaded_at='2026-10-30 12:10:00'),
 row('pickup_only',num_docks_available=None),
 row('empty',num_bikes_available=0,num_docks_available=0),
 row('disabled_conflict'),row('disabled_conflict',num_bikes_disabled=1,snapshot_id='b'),
]
setup='''CREATE TABLE fct_station_availability (
source_id Nullable(String), station_id Nullable(String), feed_updated_at Nullable(DateTime('UTC')),
last_reported_at Nullable(DateTime('UTC')), loaded_at DateTime64(3,'UTC'), snapshot_id Nullable(String),
num_bikes_available Nullable(UInt32), num_ebikes_available Nullable(UInt32), num_bikes_disabled Nullable(UInt32),
num_docks_available Nullable(UInt32), num_docks_disabled Nullable(UInt32),
is_installed Nullable(UInt32), is_renting Nullable(UInt32), is_returning Nullable(UInt32)
) ENGINE=Memory;
'''
setup+='INSERT INTO fct_station_availability VALUES '+',\n'.join(rows)+';\n'
checks=[
("station_id='repeat'", 'observation_count=1 AND conflicting_observations=0 AND pickup_usable_observations=1 AND avg_bikes_available=5', 'late repeat keeps first valid arrival'),
("station_id='conflict'", 'observation_count=1 AND conflicting_observations=1 AND pickup_usable_observations=0 AND return_usable_observations=0 AND isNull(avg_bikes_available)', 'conflict excluded'),
("station_id='null_conflict'", 'conflicting_observations=1 AND pickup_usable_observations=0', 'NULL versus zero conflict'),
("station_id='different_sources'", 'observation_count=2 AND conflicting_observations=0 AND avg_bikes_available=6', 'different sources remain separate'),
("station_id='stale'", 'fresh_at_load_observations=0 AND isNull(avg_bikes_available) AND isNull(zero_bike_observation_share)', 'stale remains unknown'),
("station_id='pickup_only'", 'pickup_usable_observations=1 AND return_usable_observations=0 AND isNull(avg_docks_available) AND avg_bikes_available=5', 'independent sides'),
("station_id='empty'", 'zero_bike_observation_share=1 AND zero_dock_observation_share=1 AND avg_bikes_available=0', 'valid zero'),
("station_id='disabled_conflict'", 'conflicting_observations=1 AND pickup_usable_observations=0', 'other measures detect conflict'),
]
sql=setup+'CREATE VIEW agg_station_hourly_availability AS '+model+';\n'
for predicate, condition, description in checks:
    sql+=f"SELECT throwIf(countIf({condition}) != 1, '{description}') FROM agg_station_hourly_availability WHERE {predicate};\n"
for f in sorted((root/'tests').glob('availability_*.sql')):
    test=env.from_string(f.read_text()).render().rstrip().rstrip(';')
    sql+=f"SELECT throwIf(count() > 0, '{f.stem}') FROM ({test});\n"
sql+='INSERT INTO fct_station_availability VALUES '+row('repeat',feed_updated_at='2026-10-30 12:01:00',last_reported_at='2026-10-30 12:01:00',loaded_at='2026-10-30 12:01:05',snapshot_id='c')+';\n'
sql+="SELECT throwIf(observation_count != 2 OR pickup_usable_observations != 2 OR observed_minute_buckets != 2, 'view sees new observation') FROM agg_station_hourly_availability WHERE station_id='repeat';\n"
@unittest.skipUnless(shutil.which('clickhouse'), 'ClickHouse local executable is required')
class AvailabilityRegressionTests(unittest.TestCase):
    def test_repeated_updates_quality_and_live_view(self):
        with tempfile.TemporaryDirectory(prefix='availability-local-') as path:
            result = subprocess.run(
                ['clickhouse', 'local', '--path', path, '--multiquery'],
                input=sql, text=True, capture_output=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
