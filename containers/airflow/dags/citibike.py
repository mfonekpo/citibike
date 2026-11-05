import json
from datetime import date, datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="citibike_monthly_batch",
    description="Load a verified Citi Bike batch and build analytics.",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "release_month": Param(
            "2026-09",
            type="string",
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=1),
        "pool": "citibike_warehouse",
    },
    tags=["citibike", "batch"],
) as dag:

    # TASK 1: Resolve the selected release month

    @task(multiple_outputs=True)
    def resolve_release():
        context = get_current_context()
        release_month = context["params"]["release_month"]

        reporting_start = date.fromisoformat(
            f"{release_month}-01"
        )

        year = reporting_start.year
        month = reporting_start.month

        if month == 12:
            reporting_end = date(year + 1, 1, 1)
        else:
            reporting_end = date(year, month + 1, 1)

        return {
            "release_month": release_month,

            "manifest_key": (
                f"manifests/batch-manifest-{release_month}.json"
            ),

            "data_prefix": (
                f"{year:04d}{month:02d}-citibike-tripdata/"
            ),

            "dbt_vars": json.dumps({
                "trip_reporting_start_date":
                    reporting_start.isoformat(),

                "trip_reporting_end_date":
                    reporting_end.isoformat(),
            }),
        }

    release = resolve_release()

    # TASK 2: Validate the manifest and source files

    validate_manifest_and_files = BashOperator(
        task_id="validate_manifest_and_files",

        bash_command="""\
set -e

/opt/citibike-venv/bin/python - <<'PY'
import os
from contextlib import closing
from src.manifest_reader import read_batch_manifest
from utils.s3_connect import get_minio_client, required_env

with closing(get_minio_client()) as client:
    manifest = read_batch_manifest(
        client,
        required_env("MINIO_BUCKET"),
        os.environ["CITIBIKE_MANIFEST_KEY"],
    )

expected = os.environ["CITIBIKE_RELEASE_MONTH"]
actual = manifest["release_month"]

if actual != expected:
    raise ValueError(
        f"Expected release {expected}, got {actual}"
    )

print(f"Manifest month verified: {actual}")
PY

/opt/citibike-venv/bin/python -m src.data_loader \
    --manifest-key "$CITIBIKE_MANIFEST_KEY" \
    --data-prefix "$CITIBIKE_DATA_PREFIX" \
    --validate-only
""",

        cwd="/opt/citibike",

        env={
            "CITIBIKE_RELEASE_MONTH": release["release_month"],
            "CITIBIKE_MANIFEST_KEY": release["manifest_key"],
            "CITIBIKE_DATA_PREFIX": release["data_prefix"],
        },

        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    # TASK 3: Load and publish the selected month

    load_and_publish = BashOperator(
        task_id="load_and_publish",

        bash_command="""\
exec /opt/citibike-venv/bin/python -m src.data_loader \
    --manifest-key "$CITIBIKE_MANIFEST_KEY" \
    --data-prefix "$CITIBIKE_DATA_PREFIX"
""",

        cwd="/opt/citibike",

        env={
            "CITIBIKE_MANIFEST_KEY": release["manifest_key"],
            "CITIBIKE_DATA_PREFIX": release["data_prefix"],
        },

        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    # TASK 4: Execute dbt transformations

    build_analytics = BashOperator(
        task_id="build_analytics",

        bash_command="""\
exec /opt/citibike-venv/bin/dbt build \
    --project-dir "$DBT_PROJECT_DIR" \
    --profiles-dir "$DBT_PROFILES_DIR" \
    --vars "$CITIBIKE_DBT_VARS"
""",

        cwd="/opt/citibike/citibike_analytics",

        env={
            "CITIBIKE_DBT_VARS": release["dbt_vars"],
        },

        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    # TASK DEPENDENCIES

    (
        release
        >> validate_manifest_and_files
        >> load_and_publish
        >> build_analytics
    )