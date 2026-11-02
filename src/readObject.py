from utils.logging_config import logger


def get_object_keys(
    minio_client,
    bucket: str,
    prefix: str = "",
) -> list[str]:
    paginator = minio_client.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.lower().endswith(".csv"):
                logger.info("appending CSV file: %s", key)
                keys.append(key)

    return sorted(keys)
