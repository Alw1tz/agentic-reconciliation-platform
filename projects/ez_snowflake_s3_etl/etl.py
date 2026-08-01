"""A minimal Extract + Load, no orchestration -- tier 1, project 3 of the
learning curriculum (README.md in this folder). S3 and Snowflake talking
directly to each other, so those two systems are understood on their own
before Airflow orchestrates this same kind of step in top_dynamic_etl_pipeline.

Extract: write a small CSV to S3, then read it back (standing in for "the
file already landed in S3 from somewhere else", same role S3 plays in
reconciliation_dag.py and minsait_hybrid).
Load: create a Snowflake table and insert the rows.
"""
import csv
import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from projects._shared.connections import get_s3_client, get_snowflake_connection

load_dotenv()

S3_KEY = "learning/ez_snowflake_s3_etl/sample_orders.csv"
TABLE_NAME = "EZ_HELLO_ETL"

SAMPLE_ROWS = [
    {"order_id": 1, "customer": "acme corp", "amount_usd": "120.50"},
    {"order_id": 2, "customer": "widget co", "amount_usd": "89.00"},
    {"order_id": 3, "customer": "acme corp", "amount_usd": "310.25"},
]


def extract() -> list[dict]:
    """Write sample rows to S3 as CSV, then read them back -- the round
    trip through S3 is the point: this is "extracting" a file that in a
    real pipeline landed there from an external system, not generating
    data locally."""
    bucket = os.environ["S3_BUCKET"]
    s3 = get_s3_client()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(SAMPLE_ROWS[0].keys()))
    writer.writeheader()
    writer.writerows(SAMPLE_ROWS)
    s3.put_object(Bucket=bucket, Key=S3_KEY, Body=buf.getvalue().encode("utf-8"))
    print(f"Wrote {len(SAMPLE_ROWS)} rows to s3://{bucket}/{S3_KEY}")

    obj = s3.get_object(Bucket=bucket, Key=S3_KEY)
    rows = list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8"))))
    print(f"Read {len(rows)} rows back from S3")
    return rows


def load(rows: list[dict]) -> None:
    """CREATE OR REPLACE (not CREATE IF NOT EXISTS) makes re-running this
    script idempotent -- each run starts from a clean table instead of
    accumulating duplicate inserts, same principle as the real DAG's
    RUN_ID-scoped loads, simplified since there's no historical RAW layer
    to preserve here."""
    con = get_snowflake_connection()
    try:
        cur = con.cursor()
        cur.execute(f"""
            CREATE OR REPLACE TABLE {TABLE_NAME} (
                ORDER_ID NUMBER,
                CUSTOMER VARCHAR,
                AMOUNT_USD DECIMAL(10,2)
            )
        """)
        values = ", ".join(
            f"({r['order_id']}, '{r['customer']}', {r['amount_usd']})" for r in rows
        )
        cur.execute(f"INSERT INTO {TABLE_NAME} VALUES {values}")
        con.commit()
        print(f"Loaded {len(rows)} rows into {TABLE_NAME}")

        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        (count,) = cur.fetchone()
        print(f"Verified: {TABLE_NAME} now has {count} row(s)")
    finally:
        con.close()


if __name__ == "__main__":
    load(extract())
