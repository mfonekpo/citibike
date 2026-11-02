# Citi Bike Analytics

dbt models for a ClickHouse warehouse combining historical Citi Bike rides with near-real-time station observations.

This project provides the Silver and Gold layers of the warehouse. It cleans Bronze inputs, resolves station identifiers, builds facts and dimensions, and exposes metrics for a future dashboard.

**Current state:** 13 models and 8 singular SQL tests. The project has passed dbt build in the working environment and is invoked by the successful citibike_monthly_batch Airflow DAG. Dashboard implementation and automatic source downloads are deferred.

See the [root README](../README.md) for infrastructure, credentials, raw-table setup, MinIO uploads, NiFi configuration, and Airflow startup.

## Contents

- [Warehouse structure](#warehouse-structure)
- [Run the project](#run-the-project)
- [Model inventory and lineage](#model-inventory-and-lineage)
- [Silver transformations](#silver-transformations)
- [Dimensions and facts](#dimensions-and-facts)
- [Metrics and marts](#metrics-and-marts)
- [Configuration](#configuration)
- [Refresh behavior](#refresh-behavior)
- [Data quality tests](#data-quality-tests)
- [Example analytical queries](#example-analytical-queries)
- [Assumptions and limitations](#assumptions-and-limitations)

## Warehouse structure

All relations currently reside in **citibike**. Bronze, Silver, and Gold describe responsibilities; they are not separate ClickHouse databases.

| Layer | Resources | Role |
| --- | --- | --- |
| Bronze | Four source tables | Original CSV fields or complete source JSON responses. |
| Silver | Four staging views | Typed, unpacked, normalized source data. |
| Gold | Dimensions, facts, hourly aggregates, current-state marts | Reusable business entities and metrics. |
| Gold support | Intermediate demand baseline | Historical calendar-based averages for priority scoring. |

The approach is **dimensional modeling**, using shared station/region dimensions and separate facts for trips and station observations. The facts have different grains and should not be joined directly row-for-row.

### Source declarations

The source name used in dbt is citibike_raw.

| Logical dbt source table | Physical ClickHouse relation |
| --- | --- |
| raw_citibike_tripdata | citibike.raw_citibike_tripdata_v2 |
| stream_stations_status_raw | citibike.stream_stations_status_raw |
| stream_station_information_raw | citibike.stream_station_information_raw |
| stream_system_regions_raw | citibike.stream_system_regions_raw |

The trip declaration uses identifier: raw_citibike_tripdata_v2. Therefore:

~~~sql
{{ source('citibike_raw', 'raw_citibike_tripdata') }}
~~~

resolves to the new partitioned table while keeping the logical source name stable.

Source declarations live in models/staging/_src_citibike.yml. Their column type entries document expected types; they do not themselves cast values or enforce the separate YAML schema contract.

### Station identifiers

The monthly CSV and GBFS feeds use different identifiers:

~~~text
CSV start_station_id / end_station_id
                    │
                    ▼
GBFS station_information.short_name
                    │
                    ▼
dim_stations.station_code → dim_stations.station_id
                                      │
                                      ▼
                           GBFS station_status.station_id
~~~

Station codes remain strings. For example, a code containing a decimal point is still an identifier, not a measurement.

The current trip joins use exact string equality. The numeric-code fallback described in the project manifest is not implemented. Unmatched trip endpoints are retained and flagged.

## Run the project

### Prerequisites

Before building analytics:

1. Start ClickHouse and the supporting services.
2. Create the current Bronze tables, including raw_citibike_tripdata_v2.
3. Publish the historical batch through the loader or Airflow.
4. Collect station information, regions, and status responses through NiFi.
5. Create a valid profiles.yml as shown in the root README.
6. Supply the ClickHouse environment variables and install dbt packages.

The root requirements pin dbt 2.0.9. Airflow executes the CLI from /opt/citibike-venv/bin/dbt. The project name and profile name are both citibike_analytics.

### Recommended: use the Airflow worker environment
