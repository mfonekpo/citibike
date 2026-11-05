import argparse
import csv
import hashlib
import io
import json
from contextlib import closing
from datetime import date

from botocore.exceptions import ClientError

from utils.logging_config import logger
from utils.s3_connect import get_minio_client, required_env

EXPECTED_COLUMNS = {
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
}


def inspect_csv(client, bucket: str, obj: dict) -> dict:
    """Validate a CSV stored in MinIO and calculate its metadata."""

    key = obj["Key"]
    filename = key.rsplit("/", 1)[-1]

    etag = obj["ETag"]
    expected_size = obj["Size"]

    # Pass 1: Calculate SHA-256 without loading the file into memory.
    response = client.get_object(
        Bucket=bucket,
        Key=key,
        IfMatch=etag,
    )

    digest = hashlib.sha256()
    size_bytes = 0

    with closing(response["Body"]) as body:
        for chunk in iter(
            lambda: body.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)
            size_bytes += len(chunk)

    if size_bytes != expected_size:
        raise ValueError(
            f"Size changed during inspection: {key}"
        )

    # Pass 2: Validate CSV structure and count records.
    response = client.get_object(
        Bucket=bucket,
        Key=key,
        IfMatch=etag,
    )

    with closing(response["Body"]) as body:
        with io.TextIOWrapper(
            body,
            encoding="utf-8-sig",
            newline="",
        ) as text:

            reader = csv.reader(text, strict=True)
            columns = next(reader, None)

            if (
                columns is None
                or len(columns) != len(EXPECTED_COLUMNS)
                or set(columns) != EXPECTED_COLUMNS
            ):
                raise ValueError(
                    f"Unexpected CSV header: {filename}"
                )

            row_count = 0

            for record_number, row in enumerate(
                reader, start=1
            ):
                if len(row) != len(columns):
                    raise ValueError(
                        f"{filename}: data record {record_number} "
                        f"has {len(row)} fields; "
                        f"expected {len(columns)}."
                    )

                row_count += 1

    return {
        "filename": filename,
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "row_count": row_count,
        "columns": columns,
    }



def build_manifest(
    client,
    bucket: str,
    release_month: str,
    expected_files: int,
) -> dict:
    """Build a deterministic manifest from MinIO objects."""

    date.fromisoformat(f"{release_month}-01")

    if expected_files < 1:
        raise ValueError(
            "expected_files must be positive."
        )

    month = release_month.replace("-", "")

    prefix = f"{month}-citibike-tripdata/"

    paginator = client.get_paginator("list_objects_v2")

    objects = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key == prefix:
                continue

            filename = key.removeprefix(prefix)

            if (
                "/" in filename
                or not filename.endswith(".csv")
                or not filename.startswith(
                    f"{month}-citibike-tripdata"
                )
            ):
                raise ValueError(
                    f"Unexpected object in release prefix: {key}"
                )

            objects.append(obj)

    objects.sort(key=lambda obj: obj["Key"])

    if len(objects) != expected_files:
        raise ValueError(
            f"Expected {expected_files} CSV files, "
            f"found {len(objects)}."
        )

    files = [
        inspect_csv(client, bucket, obj)
        for obj in objects
    ]

    manifest = {
        "manifest_version": 1,
        "source_id": "citibike_nyc_trip_history",
        "release_month": release_month,
        "file_count": len(files),
        "total_rows": sum(
            item["row_count"] for item in files
        ),
        "files": files,
    }

    canonical_json = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    manifest["batch_id"] = hashlib.sha256(
        canonical_json
    ).hexdigest()

    return manifest


def publish_manifest(
    client,
    bucket: str,
    manifest: dict,
) -> str:
    """Save the manifest in MinIO without overwriting it."""

    key = (
        f"manifests/"
        f"batch-manifest-{manifest['release_month']}.json"
    )

    payload = (
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        ) + "\n"
    ).encode("utf-8")

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            IfNoneMatch="*",
        )

        return f"Created s3://{bucket}/{key}"

    except ClientError as exc:
        code = exc.response.get(
            "Error", {}
        ).get("Code", "")

        if code not in {
            "PreconditionFailed",
            "412",
        }:
            raise

    # An existing manifest must match the new one.
    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )

    with closing(response["Body"]) as body:
        existing = json.load(body)

    if existing != manifest:
        raise ValueError(
            f"Existing manifest differs: "
            f"s3://{bucket}/{key}"
        )

    return (
        f"Manifest already exists unchanged: "
        f"s3://{bucket}/{key}"
    )



def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--release-month",
        required=True,
    )

    parser.add_argument(
        "--expected-files",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--publish",
        action="store_true",
    )

    args = parser.parse_args()

    with closing(get_minio_client()) as client:
        bucket = required_env("MINIO_BUCKET")

        manifest = build_manifest(
            client=client,
            bucket=bucket,
            release_month=args.release_month,
            expected_files=args.expected_files,
        )

        logger.info(f"Release month: {manifest['release_month']}")
        logger.info(f"Files: {manifest['file_count']}")
        logger.info(f"Total rows: {manifest['total_rows']}")
        logger.info(f"Batch ID: {manifest['batch_id']}")

        if args.publish:
            logger.info("Publishing manifest...")
            print(
                publish_manifest(
                    client,
                    bucket,
                    manifest,
                )
            )
        else:
            print(
                "Preview only. Nothing was written to MinIO."
            )


if __name__ == "__main__":
    main()