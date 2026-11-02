import hashlib
import json
from contextlib import closing
from datetime import date

from src.batch_manifest import EXPECTED_COLUMNS


def read_batch_manifest(minio_client, bucket: str, manifest_key: str) -> dict:
    response = minio_client.get_object(Bucket=bucket, Key=manifest_key)

    with closing(response["Body"]) as body:
        manifest = json.load(body)

    if manifest["manifest_version"] != 1:
        raise ValueError("Unsupported manifest version.")

    if manifest["source_id"] != "citibike_nyc_trip_history":
        raise ValueError("Unexpected manifest source.")

    payload = dict(manifest)
    recorded_id = payload.pop("batch_id")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    actual_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    if actual_id != recorded_id:
        raise ValueError("Manifest batch_id does not match its contents.")

    date.fromisoformat(f'{manifest["release_month"]}-01')

    entries = manifest["files"]
    if not entries or manifest["file_count"] != len(entries):
        raise ValueError("Manifest file_count is invalid.")

    names = [entry["filename"] for entry in entries]
    if len(names) != len(set(names)):
        raise ValueError("Manifest contains duplicate filenames.")

    release_prefix = (
        manifest["release_month"].replace("-", "") + "-citibike-tripdata"
    )

    for entry in entries:
        name = entry["filename"]

        if (
            "/" in name
            or "\\" in name
            or not name.startswith(release_prefix)
            or not name.endswith(".csv")
        ):
            raise ValueError(f"Unexpected release filename: {name}")

        if (
            len(entry["columns"]) != len(EXPECTED_COLUMNS)
            or set(entry["columns"]) != EXPECTED_COLUMNS
        ):
            raise ValueError(f"Unexpected manifest columns: {name}")

        if type(entry["row_count"]) is not int or entry["row_count"] < 0:
            raise ValueError(f"Invalid row_count: {name}")

        if type(entry["size_bytes"]) is not int or entry["size_bytes"] <= 0:
            raise ValueError(f"Invalid size_bytes: {name}")

    if manifest["total_rows"] != sum(entry["row_count"] for entry in entries):
        raise ValueError("Manifest total_rows does not match its files.")

    return manifest


def verify_manifest_objects(minio_client, bucket, manifest, data_prefix):
    prefix = data_prefix.rstrip("/")
    verified_files = []

    for entry in manifest["files"]:
        key = (
            f'{prefix}/{entry["filename"]}'
            if prefix
            else entry["filename"]
        )

        response = minio_client.get_object(Bucket=bucket, Key=key)
        digest = hashlib.sha256()
        byte_count = 0

        with closing(response["Body"]) as body:
            for block in iter(lambda: body.read(1024 * 1024), b""):
                digest.update(block)
                byte_count += len(block)

        if byte_count != entry["size_bytes"]:
            raise ValueError(f"File size mismatch: {key}")

        if digest.hexdigest() != entry["sha256"]:
            raise ValueError(f"SHA-256 mismatch: {key}")

        verified_files.append({
            "key": key,
            "etag": response["ETag"],
            "row_count": entry["row_count"],
        })

    return verified_files