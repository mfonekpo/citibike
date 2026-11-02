# Citi Bike ClickHouse analytics

This dbt project cleans raw Citi Bike data in ClickHouse and exposes station usage,
availability, and advisory rebalancing metrics. Dashboard setup is intentionally
outside the current scope.

## Medallion layers

The layers are logical groups of relations in the existing `citibike` database;
this change does not move ingestion to new databases.

| Layer | Relations | Purpose |
| --- | --- | --- |
| Bronze | `raw_citibike_tripdata`, `stream_stations_status_raw`, `stream_station_information_raw`, `stream_system_regions_raw` | Original trip fields and raw GBFS response payloads, with ingestion metadata. |
| Silver | `stg_citibike__*` views | Parse types, unpack JSON, normalize missing values, and apply the existing snapshot retry handling. |
| Gold | `dim_regions`, `dim_stations`, `fct_trips`, `fct_station_availability`, aggregates and marts | Conformed station identifiers, facts, and metrics for later dashboards. |
| Gold support | `int_station_hourly_demand_baseline` | Historical weekday/hour demand used by the priority mart. |

NiFi polls GBFS for station-status snapshots and reference information. These are
near-real-time observations of inventory, not individual trip events. The monthly
CSV release supplies completed rides. Both sources land in Bronze before dbt
transforms them inside ClickHouse: this is ELT.

Trip station codes match GBFS `station_information.short_name`; the station
dimension resolves them to GBFS `station_id`. Unmatched trip endpoints remain in
`fct_trips`; each hourly station count includes only endpoints that mapped.

## Models 6, 7 and 8

### 6. `agg_station_hourly_usage`

**Grain:** one station and one UTC hour with recorded endpoint activity.

- Departures use `started_at`; arrivals use `ended_at`.
- `net_outflow = departures - arrivals`.
- `station_movements = departures + arrivals`. A round trip to the same station
  contributes two movements; this is not a count of distinct rides.
- UTC buckets remain distinct across daylight-saving changes. Local labels use
  New York time.

This remains a dbt **table**, refreshed when built. Its counting logic is unchanged.
It includes all valid mapped trip endpoints in the fact. The separate baseline
selects an explicit historical reporting period from these hourly counts.

### Historical demand baseline

`int_station_hourly_demand_baseline` is a table with one row per station, ISO
weekday (Monday = 1), and local hour. The default reporting period is September 1,
2026 inclusive through October 1, 2026 exclusive, interpreted in New York time.

Averages include zero-trip dates. For example, if a station has 20 departures
across four occurrences of Friday 09:00, its average is 5, even if all 20 trips
happened on just one Friday. Using only active dates would overstate demand.

`baseline_days` counts distinct dates; `baseline_hour_opportunities` counts actual
UTC hours corresponding to that local weekday/hour. This distinguishes the repeated
fall-back hour from the missing spring-forward hour. The latter is not fabricated
as a zero-trip hour.

A station with no mapped trips anywhere in the reporting period has NULL averages
and `station_in_reporting_data = 0`. For observed stations, zero-trip dates assume
the station operated throughout the selected period. Historical opening/closing
information is unavailable. This assumption is exposed in `coverage_basis`.

Use a fully loaded reporting period: the calendar denominator does not independently
prove that every source file was loaded. The configured dates are also exposed in
the baseline and priority mart for inspection.

### 7. `agg_station_hourly_availability`

**Grain:** one station and one UTC source-feed hour with stored observations.
This is now a **view**, so newly ingested observations can affect results without
rebuilding the hourly aggregate.

Repeated polls of an identical station update are collapsed by source, station,
and `feed_updated_at`, retaining the earliest warehouse arrival. A delayed copy
therefore cannot erase an earlier timely observation. If that group contains
conflicting station values, `conflicting_observations` increases and that update
is excluded from usable measurements. Bronze remains available for investigation.

An observation is usable only when the station is installed, the relevant service
is enabled, the relevant inventory count is known, and both source timestamps are
within the configured window around its first arrival. Pickup and return conditions
are assessed independently. Freshness here is measured at arrival, not against the
current clock, so old but timely history stays valid.

- Inventory averages and minima use usable observations only.
- `zero_bike_observation_share` and `zero_dock_observation_share` range from 0 to 1.
  They are NULL if the relevant side has no usable observations.
- `observed_minute_bucket_share` measures minutes containing any observed source
  update, including unusable updates. It is not usable coverage or continuous uptime.
- An observation share is not an outage-duration percentage. Missing hours remain
  missing; they are not automatically hours of zero availability.

### 8. `mart_station_rebalancing_priority`

**Grain:** one station using its latest status. This remains a **view**.

The mart compares the current New York weekday/hour with the historical baseline.
It no longer searches a September batch for trips in the last 24 wall-clock hours.
Recent availability uses the six completed UTC hours before the current hour and
combines counts before calculating shares.

The existing scoring idea is retained as an explicit heuristic. For a side with
low inventory:

```
score = 70 * shortage_fraction
      + 20 * min(historical_average_hourly_trips / reference_trips_per_hour, 1)
      + 10 * recent_zero_inventory_observation_share
```

The low-inventory threshold is 20% of current station capacity, rounded upward,
with a minimum of 2 and capped at capacity. The default reference activity is 20
trips per hour. These weights and cutoffs are project choices, not calibrated
probabilities or a forecast of demand in the remainder of the current hour.

Each side requires current usable data, known positive capacity, a supported
historical baseline, and at least one usable observation in the preceding six
completed hours. A valid side at or above its inventory threshold scores 0. A side
missing required inputs scores NULL. During startup it is normal for a station to
have insufficient history until usable observations exist in a completed hour.

The output exposes:

- Historical average departures/arrivals, baseline dates, days, and hour counts.
- Recent usable observation counts and nullable zero-inventory shares.
- `is_pickup_priority_eligible` and `is_return_priority_eligible`.
- `add_bikes_score`, `return_capacity_score`, and `priority_score` (0–100 when known).
- `assessment_status`, `recommended_action`, and `priority_band`.

`priority_score` is the larger eligible side score, or NULL when neither can be
assessed. A positive score from only one side is explicitly marked `PARTIAL_DATA`.
`NO_ACTION` requires a complete assessment with both scores zero. A healthy known
side with an unknown opposite side yields `CHECK_UNASSESSED_SIDE` instead.

Low return capacity does not prove that all docks contain available bikes: disabled
docks and bikes can also matter. The former `remove_bikes_score` is therefore named
`return_capacity_score`, with action `RESTORE_RETURN_CAPACITY`. If both sides score
positively, the action is `CHECK_BOTH_SIDES`. These are investigation priorities,
not automatic dispatch instructions.

The removed `departures_last_24h` / `arrivals_last_24h` fields are replaced by
`historical_avg_departures` / `historical_avg_arrivals`. No dashboard currently
depends on the old names.

## Configuration

Edit `vars` in `dbt_project.yml` before building a different reporting period:

| Variable | Default | Meaning |
| --- | --- | --- |
| `analytics_timezone` | `America/New_York` | Baseline and availability calendar labels. Trip timestamp interpretation remains New York time in staging. |
| `trip_reporting_start_date` | `2026-09-01` | Inclusive historical local date. |
| `trip_reporting_end_date` | `2026-10-01` | Exclusive historical local date. |
| `minimum_baseline_days` | `3` | Required dates for the selected weekday/hour. |
| `priority_reference_trips_per_hour` | `20` | Hourly activity that saturates the historical score component; must be positive. |
| `max_report_age_seconds` | `300` | Maximum source timestamp age at arrival or current evaluation. |
| `allowed_future_clock_skew_seconds` | `60` | Permitted future source timestamp offset. |

The latest-availability view and historical hourly view use the same freshness
variables. Changing a view definition/configuration requires running dbt again.

## Run the updated models

Use your existing dbt environment and ClickHouse profile. From the repository root:

```bash
cd citibike_analytics
dbt build --select +mart_station_rebalancing_priority
```

The `+` includes upstream models, including dimensions, hourly usage and the new
baseline; `build` also runs the selected models' dbt tests. The build converts hourly
availability from its previous stored table into a view. The raw tables are inputs
and are not reloaded by this command. Run `dbt deps` first only if the configured
packages are not already installed.

For a new historical reporting period, set both dates together and rebuild the
same selection. Do not change only the downstream view while retaining a baseline
table from a different configuration.

After a successful build, inspect:

```sql
SELECT assessment_status, count()
FROM citibike.mart_station_rebalancing_priority
GROUP BY assessment_status;

SELECT station_name, historical_avg_departures, historical_avg_arrivals,
       assessment_status, priority_score, recommended_action
FROM citibike.mart_station_rebalancing_priority
WHERE priority_score IS NOT NULL AND priority_score > 0
ORDER BY priority_score DESC
LIMIT 20;
```

An empty priority list can legitimately mean no supported current assessment or no
positive score. Inspect `assessment_status` before interpreting it as healthy service.

## Validation

The dbt tests cover hourly grains and metric bounds, baseline calendar denominators,
priority eligibility and NULL handling, configuration bounds, and duplicate/missing
trip IDs. The trip-ID test catches repeated CSV loads that would inflate demand; it
does not automatically remove those rows.

Offline regression tests render the real SQL and execute synthetic fixtures with
`clickhouse local`; they need Python, Jinja2, and a ClickHouse executable. From the
repository root:

```bash
python3 -m unittest discover -s tests -p 'test_gold_*.py' -v
```

No credentials, network service, or production warehouse is needed for those checks.
They cover the delayed-copy regression, conflicts, live view updates, independent
pickup/return quality, zero-trip dates, reporting boundaries, daylight-saving time,
and incomplete priority inputs. Fixture checks do not replace the real dbt build
against your data. Views over growing snapshot history may eventually need a tested
incremental/materialized implementation if query cost becomes significant.

## Where Airflow fits

Airflow schedules and coordinates the batch workflow; dbt performs the warehouse
transformations. NiFi continues collecting streaming snapshots independently.

```
Airflow: discover/download new monthly release
         -> check file checksum / previous success
         -> load raw rows into ClickHouse Bronze
         -> dbt build (Silver, Gold, tests)
         -> record successful processing / report failures

NiFi:    poll GBFS -> ClickHouse Bronze -> existing Silver/Gold views at query time
```

There is no Citi Bike Airflow DAG in this repository yet. These model fixes do not
complete that orchestration requirement. The next implementation step is a DAG
that invokes the loader and dbt with the required files, dependencies and credentials
available to its worker.

The current Python CSV loader appends rows. Before scheduling automatic ingestion
or task retries, add a file-checksum ledger and a clear replay strategy so the same
release is not inserted twice. Separately, schedule dimension and batch-table
refreshes: a view over a stored dimension or baseline does not refresh that table.
A monthly batch build and a suitable station/region dimension refresh schedule are
different jobs. Airflow need not trigger every NiFi poll.

References: [dbt materializations](https://docs.getdbt.com/docs/build/materializations),
[dbt build](https://docs.getdbt.com/reference/commands/build), and
[Airflow core concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html).
