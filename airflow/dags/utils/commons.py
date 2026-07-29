"""Reusable utilities for all DAGs. Plain boto3 (env-var credentials) rather
than an Airflow aws_default connection — keeps this repo's setup to a single
.env, same values the SparkSubmitOperator's s3a conf also reads.
"""
import io
import os

import boto3


def get_s3_client():
    return boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


def upload_csv_to_s3(bucket: str, key: str, rows: list[dict]) -> None:
    """Upload a list of dicts as CSV to S3."""
    import csv

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    get_s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))


def download_from_s3(bucket: str, key: str) -> str:
    obj = get_s3_client().get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def list_s3_objects(bucket: str, prefix: str = "") -> list[str]:
    resp = get_s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]
