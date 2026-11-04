from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="citibike_baserow_maintenance",
    description="Load Baserow maintenance tickets and build their analytics.",
    start_date=datetime(2026, 11, 4, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
        "pool": "citibike_warehouse",
    },
    tags=["citibike", "baserow", "maintenance"],
) as dag:

    load_baserow = BashOperator(
        task_id="load_baserow",
        bash_command=(
            "exec /opt/citibike-venv/bin/python "
            "-u -m src.baserow_loader"
        ),
        cwd="/opt/citibike",
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    build_baserow_analytics = BashOperator(
        task_id="build_baserow_analytics",
        bash_command=r"""
set -euo pipefail

exec /opt/citibike-venv/bin/dbt build \
    --project-dir "$DBT_PROJECT_DIR" \
    --profiles-dir "$DBT_PROFILES_DIR" \
    --select "+agg_station_maintenance" \
    --vars "baserow_maintenance_table_id: ${BASEROW_TABLE_ID:?BASEROW_TABLE_ID is required}"
""",
        cwd="/opt/citibike/citibike_analytics",
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    load_baserow >> build_baserow_analytics