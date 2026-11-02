import argparse
import csv
import hashlib
import json
from datetime import date
from pathlib import Path


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


def file_sha256(path: Path) -> str:
    """Calculate a fingerprint without reading the whole file into memory."""
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def inspect_csv(path: Path) -> dict:
    """Describe one CSV while preserving its original contents."""
    before = path.stat()
    checksum = file_sha256(path)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, strict=True)
        columns = next(reader, None)

        if (
            columns is None
            or len(columns) != len(EXPECTED_COLUMNS)
            or set(columns) != EXPECTED_COLUMNS
        ):
            raise ValueError(f"Unexpected CSV header: {path.name}")

        row_count = 0

        for record_number, row in enumerate(reader, start=1):
            if len(row) != len(columns):
                raise ValueError(
                    f"{path.name}: data record {record_number} "
                    f"has {len(row)} fields; expected {len(columns)}."
                )

            row_count += 1

    after = path.stat()

    # Catch ordinary changes made while the file was being inspected.
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise RuntimeError(f"File changed during inspection: {path.name}")

    return {
        "filename": path.name,
        "sha256": checksum,
        "size_bytes": after.st_size,
        "row_count": row_count,
        "columns": columns,
    }


def build_manifest(
    data_dir: Path,
    release_month: str,
    expected_files: int,
) -> dict:
    """Create a deterministic description of one release."""
    date.fromisoformat(f"{release_month}-01")

    if expected_files < 1:
        raise ValueError("expected_files must be positive.")

    if not data_dir.is_dir():
        raise NotADirectoryError(data_dir)

    paths = sorted(
        path for path in data_dir.glob("*.csv")
        if path.is_file()
    )

    if len(paths) != expected_files:
        raise ValueError(
            f"Expected {expected_files} CSV files, found {len(paths)}."
        )

    # Restrict this batch to the chosen NYC release.
    prefix = f"{release_month.replace('-', '')}-citibike-tripdata"

    for path in paths:
        if not path.name.startswith(prefix):
            raise ValueError(f"Unexpected release file: {path.name}")

    files = [inspect_csv(path) for path in paths]

    manifest = {
        "manifest_version": 1,
        "source_id": "citibike_nyc_trip_history",
        "release_month": release_month,
        "file_count": len(files),
        "total_rows": sum(item["row_count"] for item in files),
        "files": files,
    }

    canonical_json = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    manifest["batch_id"] = hashlib.sha256(canonical_json).hexdigest()

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--release-month", required=True)
    parser.add_argument("--expected-files", type=int, required=True)
    args = parser.parse_args()

    manifest = build_manifest(
        data_dir=args.data_dir,
        release_month=args.release_month,
        expected_files=args.expected_files,
    )

    with open(f"batch-manifest-{manifest['release_month']}.json", "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")

if __name__ == "__main__":
    main()