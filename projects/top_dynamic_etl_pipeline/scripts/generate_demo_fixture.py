"""Generates and uploads the small synthetic demo_multi_record fixture --
top_dynamic_etl_pipeline, tier 3 capstone
(projects/top_dynamic_etl_pipeline/README.md).

Layout matches registry.py's DEMO_MULTI_RECORD config exactly:
  'H' + batch_date(8) + store_id(5)                              = 14 chars
  'D' + transaction_id(10) + amount(8, scale 100) + sku(11)       = 30 chars
  'T' + detail_count(10)                                         = 11 chars
One header, N detail rows, one trailer -- header/trailer are the two rows
count_tolerance_pct in the registry accounts for (they're never parsed
into BRONZE by design).

Usage:
    uv run python3 projects/top_dynamic_etl_pipeline/scripts/generate_demo_fixture.py
"""
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

DEST_PREFIX = "top-dynamic-etl/demo_multi_record"
MARKER_KEY_NAME = "_SYNC_COMPLETE"
N_DETAIL_ROWS = 20


def build_fixture() -> str:
    lines = []
    lines.append("H" + "20260201" + "STR01")  # header: 14 chars
    for i in range(1, N_DETAIL_ROWS + 1):
        txn_id = f"TXN{i:07d}"  # 10 chars
        amount_cents = 1000 + i * 137  # varies per row, always < 99999999 sentinel
        amount_str = f"{amount_cents:08d}"  # 8 chars, scale 100 -> real dollars
        sku = f"SKU{i:08d}"  # 11 chars
        lines.append("D" + txn_id + amount_str + sku)
    lines.append("T" + f"{N_DETAIL_ROWS:010d}")  # trailer: 11 chars
    return "\n".join(lines) + "\n"


def main() -> None:
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    fixture = build_fixture()
    key = f"{DEST_PREFIX}/batch_20260201.txt"
    client.put_object(Bucket=bucket, Key=key, Body=fixture.encode("utf-8"))
    print(f"Uploaded {len(fixture.splitlines())} lines to s3://{bucket}/{key}")

    marker_key = f"{DEST_PREFIX}/{MARKER_KEY_NAME}"
    client.put_object(Bucket=bucket, Key=marker_key, Body=b"")
    print(f"Marker written to s3://{bucket}/{marker_key}")


if __name__ == "__main__":
    main()
