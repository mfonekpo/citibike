import json
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


BATCH = {
    "manifest_key": "batch-manifest-2026-09.json",
    "data_prefix": "202609-citibike-tripdata/",
    "reporting_start_date": "2026-09-01",
    "reporting_end_date": "2026-10-01",
}


with DAG(
    dag_id="citibike_monthly_batch",
    description="Load a verified Citi Bike batch and build analytics.",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=1),
        "pool": "citibike_warehouse",
    },
    tags=["citibike", "batch"],

) as dag:

    load_and_publish = BashOperator(
        task_id="load_and_publish",
        bash_command="""
            exec /opt/citibike-venv/bin/python -m src.data_loader \
                --manifest-key "$CITIBIKE_MANIFEST_KEY" \
                --data-prefix "$CITIBIKE_DATA_PREFIX"
        """,
        cwd="/opt/citibike",
        env={
            "CITIBIKE_MANIFEST_KEY": BATCH["manifest_key"],
            "CITIBIKE_DATA_PREFIX": BATCH["data_prefix"],
        },
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    build_analytics = BashOperator(
        task_id="build_analytics",
        bash_command="""
exec /opt/citibike-venv/bin/dbt build \
    --project-dir "$DBT_PROJECT_DIR" \
    --profiles-dir "$DBT_PROFILES_DIR" \
    --vars "$CITIBIKE_DBT_VARS"
""",
        cwd="/opt/citibike/citibike_analytics",
        env={
            "CITIBIKE_DBT_VARS": json.dumps(
                {
                    "trip_reporting_start_date":
                        BATCH["reporting_start_date"],
                    "trip_reporting_end_date":
                        BATCH["reporting_end_date"],
                }
            ),
        },
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    load_and_publish >> build_analytics