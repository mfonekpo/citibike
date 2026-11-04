"""Offline rebalancing regressions using synthetic data and the actual model SQL."""
from pathlib import Path
import subprocess
import tempfile
import shutil
import unittest
from jinja2 import Environment, StrictUndefined

root = Path(__file__).resolve().parents[1] / 'citibike_analytics'
env = Environment(undefined=StrictUndefined)
env.globals.update(config=lambda **kw: '', ref=lambda name: name, var=lambda name, default=None: default)
model = env.from_string((root/'models/marts/mart_station_rebalancing_priority.sql').read_text()).render()
setup='''CREATE TABLE mart_station_latest_availability (
station_id String, station_code Nullable(String) DEFAULT 'code', station_name Nullable(String) DEFAULT 'station',
region_id Nullable(String) DEFAULT '71', region_name Nullable(String) DEFAULT 'NYC',
latitude Nullable(Float64) DEFAULT 40.7, longitude Nullable(Float64) DEFAULT -74.0,
station_capacity Nullable(UInt32) DEFAULT 20,
feed_updated_at Nullable(DateTime('UTC')) DEFAULT '2026-10-30 13:00:00',
last_reported_at Nullable(DateTime('UTC')) DEFAULT '2026-10-30 13:00:00',
evaluated_at DateTime64(3,'UTC') DEFAULT '2026-10-30 13:00:05',
is_installed Nullable(UInt32) DEFAULT 1, is_fresh_now UInt8 DEFAULT 1,
was_fresh_at_load UInt8 DEFAULT 1, has_station_metadata UInt8 DEFAULT 1,
is_pickup_data_usable UInt8 DEFAULT 1, is_return_data_usable UInt8 DEFAULT 1,
num_bikes_available Nullable(UInt32) DEFAULT 8, num_docks_available Nullable(UInt32) DEFAULT 8
) ENGINE=Memory;
CREATE TABLE int_station_hourly_demand_baseline (
station_id String, local_weekday UInt8 DEFAULT 5, local_hour UInt8 DEFAULT 9,
baseline_days UInt64 DEFAULT 4, baseline_hour_opportunities UInt64 DEFAULT 4,
avg_departures Nullable(Float64) DEFAULT 10, avg_arrivals Nullable(Float64) DEFAULT 5,
reporting_start_date Date DEFAULT '2026-09-01', reporting_end_date_exclusive Date DEFAULT '2026-10-01',
station_in_reporting_data UInt8 DEFAULT 1
) ENGINE=Memory;
CREATE TABLE agg_station_hourly_availability (
station_id String, hour_start_utc DateTime('UTC') DEFAULT toStartOfHour(now('UTC')) - INTERVAL 1 HOUR,
pickup_usable_observations UInt64 DEFAULT 10, return_usable_observations UInt64 DEFAULT 10,
zero_bike_observations UInt64 DEFAULT 2, zero_dock_observations UInt64 DEFAULT 4
) ENGINE=Memory;
'''
def insert(table, station, overrides=None):
    values={'station_id':station}; values.update(overrides or {})
    strings=','.join('NULL' if v is None else "'"+v+"'" if isinstance(v,str) else str(v) for v in values.values())
    return f"INSERT INTO {table} ({','.join(values)}) VALUES ({strings});\n"
cases={
 'healthy':{},
 'pickup_low':{'num_bikes_available':0},
 'return_low':{'num_docks_available':0},
 'both_low':{'num_bikes_available':0,'num_docks_available':0},
 'missing_baseline':{'num_bikes_available':0},
 'short_baseline':{'num_bikes_available':0},
 'unobserved':{'num_bikes_available':0},
 'wrong_hour':{'num_bikes_available':0},
 'no_history':{'num_bikes_available':0},
 'no_pickup_history':{'num_bikes_available':0,'num_docks_available':0},
 'no_pickup_history_healthy':{},
 'pickup_only_low':{'num_bikes_available':0,'num_docks_available':None,'is_return_data_usable':0},
 'pickup_only_healthy':{'num_docks_available':None,'is_return_data_usable':0},
 'unknown':{'num_bikes_available':None,'num_docks_available':None,'is_pickup_data_usable':0,'is_return_data_usable':0},
 'stale':{'is_fresh_now':0,'is_pickup_data_usable':0,'is_return_data_usable':0},
 'out_of_service':{'is_installed':0,'is_pickup_data_usable':0,'is_return_data_usable':0},
 'no_metadata':{'has_station_metadata':0,'station_capacity':None},
 'no_capacity':{'station_capacity':0},
 'weighted_history':{'num_bikes_available':0},
 'zero_demand':{'num_bikes_available':0},
 'boundary_history':{},
 'winter_clock':{'evaluated_at':'2026-12-01 14:00:00'},
 'ny_previous_day':{'evaluated_at':'2026-10-31 02:00:00'},
}
for name, overrides in cases.items():
    setup+=insert('mart_station_latest_availability',name,overrides)
    if name!='missing_baseline':
        bo={}
        if name=='short_baseline': bo['baseline_days']=2
        if name=='unobserved': bo.update(station_in_reporting_data=0,avg_departures=None,avg_arrivals=None)
        if name=='wrong_hour': bo['local_hour']=13
        if name=='zero_demand': bo.update(avg_departures=0,avg_arrivals=0)
        if name=='winter_clock': bo.update(local_weekday=2,local_hour=9)
        if name=='ny_previous_day': bo.update(local_weekday=5,local_hour=22)
        setup+=insert('int_station_hourly_demand_baseline',name,bo)
    if name not in ('no_history','boundary_history'):
        ho={}
        if name.startswith('no_pickup_history'): ho.update(pickup_usable_observations=0,zero_bike_observations=0)
        setup+=insert('agg_station_hourly_availability',name,ho)
setup+="INSERT INTO agg_station_hourly_availability VALUES ('weighted_history', toStartOfHour(now('UTC')) - INTERVAL 2 HOUR, 90, 0, 0, 0);\n"
setup+="INSERT INTO agg_station_hourly_availability VALUES ('boundary_history', toStartOfHour(now('UTC')), 10, 10, 10, 10), ('boundary_history', toStartOfHour(now('UTC')) - INTERVAL 7 HOUR, 10, 10, 10, 10);\n"
checks={
 'healthy': "assessment_status='COMPLETE' AND recommended_action='NO_ACTION' AND priority_score=0 AND local_weekday=5 AND local_hour=9",
 'pickup_low': "recommended_action='ADD_BIKES' AND add_bikes_score=82 AND return_capacity_score=0 AND priority_score=82",
 'return_low': "recommended_action='RESTORE_RETURN_CAPACITY' AND return_capacity_score=79 AND add_bikes_score=0",
 'both_low': "recommended_action='CHECK_BOTH_SIDES' AND add_bikes_score=82 AND return_capacity_score=79",
 'missing_baseline': "assessment_status='INSUFFICIENT_BASELINE' AND isNull(historical_avg_departures) AND isNull(priority_score)",
 'short_baseline': "assessment_status='INSUFFICIENT_BASELINE' AND baseline_days=2 AND isNull(priority_score)",
 'unobserved': "assessment_status='INSUFFICIENT_BASELINE' AND isNull(priority_score)",
 'wrong_hour': "assessment_status='INSUFFICIENT_BASELINE' AND isNull(priority_score)",
 'no_history': "assessment_status='INSUFFICIENT_HISTORY' AND isNull(zero_bike_share_last_6h) AND isNull(zero_dock_share_last_6h) AND isNull(priority_score)",
 'no_pickup_history': "assessment_status='PARTIAL_DATA' AND isNull(add_bikes_score) AND return_capacity_score=79 AND recommended_action='RESTORE_RETURN_CAPACITY'",
 'no_pickup_history_healthy': "assessment_status='PARTIAL_DATA' AND isNull(add_bikes_score) AND priority_score=0 AND recommended_action='CHECK_UNASSESSED_SIDE'",
 'pickup_only_low': "assessment_status='PARTIAL_DATA' AND isNull(return_capacity_score) AND add_bikes_score=82 AND recommended_action='ADD_BIKES'",
 'pickup_only_healthy': "assessment_status='PARTIAL_DATA' AND isNull(return_capacity_score) AND add_bikes_score=0 AND recommended_action='CHECK_UNASSESSED_SIDE'",
 'unknown': "assessment_status='STATUS_UNUSABLE' AND isNull(priority_score) AND recommended_action='INSUFFICIENT_DATA'",
 'stale': "assessment_status='STALE_STATUS' AND isNull(priority_score)",
 'out_of_service': "assessment_status='OUT_OF_SERVICE' AND isNull(priority_score)",
 'no_metadata': "assessment_status='INSUFFICIENT_METADATA' AND isNull(priority_score)",
 'no_capacity': "assessment_status='INSUFFICIENT_METADATA' AND isNull(priority_score)",
 'weighted_history': "pickup_observations_last_6h=100 AND zero_bike_share_last_6h=0.02 AND add_bikes_score=80.2",
 'zero_demand': "has_supported_baseline=1 AND historical_avg_departures=0 AND add_bikes_score=72",
 'boundary_history': "assessment_status='INSUFFICIENT_HISTORY' AND pickup_observations_last_6h=0 AND isNull(priority_score)",
 'winter_clock': "assessment_status='COMPLETE' AND local_weekday=2 AND local_hour=9",
 'ny_previous_day': "assessment_status='COMPLETE' AND local_weekday=5 AND local_hour=22",
}
sql=setup+'CREATE VIEW mart_station_rebalancing_priority AS '+model+';\n'
for station, condition in checks.items():
    sql+=f"SELECT throwIf(countIf({condition}) != 1, '{station}') FROM mart_station_rebalancing_priority WHERE station_id='{station}';\n"
for f in sorted((root/'tests').glob('rebalancing_*.sql')):
    test=env.from_string(f.read_text()).render().rstrip().rstrip(';')
    sql+=f"SELECT throwIf(count() > 0, '{f.stem}') FROM ({test});\n"
@unittest.skipUnless(shutil.which('clickhouse'), 'ClickHouse local executable is required')
class RebalancingRegressionTests(unittest.TestCase):
    def test_baselines_unknown_inputs_scores_and_timezone(self):
        with tempfile.TemporaryDirectory(prefix='rebalancing-local-') as path:
            result = subprocess.run(
                ['clickhouse', 'local', '--path', path, '--multiquery'],
                input=sql, text=True, capture_output=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
