"""
Nightly reconciliation + agentic remediation pipeline.

--------
* DAG Name:
    reconciliation_dag
* Owner:
    alw1tz
* Description:
    The real, paid, full-stack rebuild of the JD's "08:00 Intelligence
    Synthesis" scenario -- and the agentic layer missing from the
    candidate's actual nightly reconciliation DAG at Rappi. Seeds a
    source/target snapshot in Snowflake with an injected mismatch,
    exports both to S3, runs a distributed Spark diff against them, then
    hands the report to a LangGraph agent pipeline (scout -> remediate
    -> post to Slack for human approval). The pipeline never writes back
    to Snowflake itself -- see CLAUDE.md for why.
"""
from __future__ import annotations

import json
import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.settings import S3_BUCKET, SPARK_CONN_ID, get_default_args

# -------------------- Globals --------------------
SOURCE_KEY = "recon-input/{{ ds_nodash }}/source_orders.csv"
TARGET_KEY = "recon-input/{{ ds_nodash }}/target_orders.csv"
DIFF_OUTPUT_PREFIX = "recon-output/{{ ds_nodash }}"

SOURCE_ROWS = [
    {"order_id": 1001, "amount_usd": "250.00"},
    {"order_id": 1002, "amount_usd": "89.50"},
    {"order_id": 1003, "amount_usd": "412.10"},
    {"order_id": 1004, "amount_usd": "15.00"},
    {"order_id": 1005, "amount_usd": "630.75"},
]
TARGET_ROWS = [
    {"order_id": 1001, "amount_usd": "250.00"},
    {"order_id": 1002, "amount_usd": "89.50"},
    {"order_id": 1003, "amount_usd": "41.21"},  # decimal-shift bug, same as Saturday's demo
    {"order_id": 1005, "amount_usd": "630.75"},  # 1004 never landed
    {"order_id": 1005, "amount_usd": "630.75"},  # and 1005 landed twice
]

# -------------------- DAG --------------------
with DAG(
    dag_id="reconciliation_dag",
    description="Snowflake -> Spark diff -> LangGraph agents -> Slack approval.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    default_args=get_default_args("alw1tz"),
    catchup=False,
    tags=["snowflake", "spark", "agents", "mcp", "slack"],
) as dag:

    @task
    def seed_and_export(ds_nodash: str = None):
        import snowflake.connector
        from utils.commons import upload_csv_to_s3

        con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        )
        try:
            cur = con.cursor()
            cur.execute("CREATE DATABASE IF NOT EXISTS " + os.environ["SNOWFLAKE_DATABASE"])
            cur.execute("CREATE SCHEMA IF NOT EXISTS " + os.environ["SNOWFLAKE_SCHEMA"])
            cur.execute("USE SCHEMA " + os.environ["SNOWFLAKE_SCHEMA"])
            cur.execute("CREATE OR REPLACE TABLE revenue_daily (order_id INT, amount_usd DECIMAL(10,2))")
            values = ", ".join(f"({r['order_id']}, {r['amount_usd']})" for r in TARGET_ROWS)
            cur.execute(f"INSERT INTO revenue_daily VALUES {values}")
            con.commit()
        finally:
            con.close()

        upload_csv_to_s3(S3_BUCKET, f"recon-input/{ds_nodash}/source_orders.csv", SOURCE_ROWS)
        upload_csv_to_s3(S3_BUCKET, f"recon-input/{ds_nodash}/target_orders.csv", TARGET_ROWS)
        print(f"Seeded revenue_daily in Snowflake and exported snapshots to s3://{S3_BUCKET}/recon-input/{ds_nodash}/")

    spark_diff = SparkSubmitOperator(
        task_id="spark_diff",
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/spark_apps/diff_source_target.py",
        application_args=[
            f"s3a://{S3_BUCKET}/{SOURCE_KEY}",
            f"s3a://{S3_BUCKET}/{TARGET_KEY}",
            f"s3a://{S3_BUCKET}/{DIFF_OUTPUT_PREFIX}",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.4.1",
        conf={
            "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "spark.jars.ivy": "/opt/airflow/.ivy2",
        },
        verbose=True,
    )

    @task
    def run_agent_pipeline(ds_nodash: str = None):
        import sys

        from utils.commons import download_from_s3, list_s3_objects

        sys.path.insert(0, "/opt/airflow/agents")
        from pipeline import run_pipeline

        prefix = f"recon-output/{ds_nodash}/"
        keys = [k for k in list_s3_objects(S3_BUCKET, prefix=prefix) if "part-" in k]
        if not keys:
            raise FileNotFoundError(f"No Spark diff output found under s3://{S3_BUCKET}/{prefix}")
        report = json.loads(download_from_s3(S3_BUCKET, keys[0]))

        final_state = run_pipeline(report)
        print(f"Pipeline finished. Approved: {final_state.get('approved')}")

    # -------------------- Task Dependencies --------------------
    seed_and_export() >> spark_diff >> run_agent_pipeline()
