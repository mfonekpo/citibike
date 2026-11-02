import os
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def required_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_minio_client():
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    return boto3.client(
        "s3",
        endpoint_url=required_env("MINIO_ENDPOINT_URL"),
        aws_access_key_id=required_env("MINIO_ROOT_USER"),
        aws_secret_access_key=required_env("MINIO_ROOT_PASSWORD"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=30,
            retries={
                "mode": "standard",
                "total_max_attempts": 3,
            },
        ),
    )