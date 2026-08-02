"""One-time bulk sync from NOAA's public ISD bucket to this repo's own S3
bucket -- top_dynamic_etl_pipeline, tier 3 capstone
(projects/top_dynamic_etl_pipeline/README.md).

Deliberately NOT an Airflow task: a real multi-GB transfer belongs to an
explicit, cost-aware operator action, not something that fires on every
DAG trigger. dag_factory.py's wait_for_sync sensor only looks for the
_SYNC_COMPLETE marker this script writes at the end -- it never sees the
individual files.

Usage:
    uv run python3 projects/top_dynamic_etl_pipeline/scripts/sync_noaa_isd.py --year 2024 --limit 1
    uv run python3 projects/top_dynamic_etl_pipeline/scripts/sync_noaa_isd.py --year 2024              # full year, ~35GB
"""
import argparse
import os
import sys

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

SOURCE_BUCKET = "noaa-isd-pds"
DEST_PREFIX = "top-dynamic-etl/noaa_isd"
MARKER_KEY_NAME = "_SYNC_COMPLETE"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only sync the first N station files -- for single-file/small-batch verification "
        "before committing to the full year's ~35GB transfer.",
    )
    args = parser.parse_args()

    bucket = os.environ["S3_BUCKET"]
    # Flat under DEST_PREFIX -- no year subfolder -- to match
    # registry.py's NOAA_ISD.s3_prefix exactly (the COPY INTO the DAG
    # runs and this script's destination both need to agree on one path).
    dest_prefix = DEST_PREFIX

    source = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    dest = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    print(f"Listing s3://{SOURCE_BUCKET}/data/{args.year}/ ...")
    paginator = source.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=f"data/{args.year}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".gz"):
                keys.append(obj["Key"])
            if args.limit and len(keys) >= args.limit:
                break
        if args.limit and len(keys) >= args.limit:
            break

    if not keys:
        print(f"No files found for year {args.year}.", file=sys.stderr)
        sys.exit(1)

    print(f"Copying {len(keys)} file(s) to s3://{bucket}/{dest_prefix}/ ...")
    total_bytes = 0
    for i, key in enumerate(keys, start=1):
        filename = key.rsplit("/", 1)[-1]
        dest_key = f"{dest_prefix}/{filename}"
        head = source.head_object(Bucket=SOURCE_BUCKET, Key=key)
        total_bytes += head["ContentLength"]
        dest.copy(
            {"Bucket": SOURCE_BUCKET, "Key": key},
            bucket,
            dest_key,
            SourceClient=source,
        )
        if i % 100 == 0 or i == len(keys):
            print(f"  {i}/{len(keys)} ({total_bytes / (1024**3):.2f} GB compressed so far)")

    marker_key = f"{dest_prefix}/{MARKER_KEY_NAME}"
    dest.put_object(Bucket=bucket, Key=marker_key, Body=b"")
    print(
        f"Done. {len(keys)} file(s), {total_bytes / (1024**3):.2f} GB compressed, "
        f"marker written to s3://{bucket}/{marker_key}"
    )


if __name__ == "__main__":
    main()
