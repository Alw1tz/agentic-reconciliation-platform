"""An Airflow DAG orchestrating a real Spark job via SparkSubmitOperator --
tier 2, project 7 of the learning curriculum
(projects/mid_airflow_spark_pipeline/README.md).

Separate from reconciliation_dag.py so the orchestration pattern itself --
stage data, hand off to Spark over s3a, read the result back -- is
understood in isolation before agent logic gets layered on top of it in
tier 3. Uses a real dataset (seaborn's tips.csv, restaurant tips -- see
airflow/dags/data/tips.csv) instead of hardcoded rows, unlike
ez_hello_dag.py's synthetic numbers.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.commons import download_from_s3, list_s3_objects, upload_csv_to_s3
from utils.settings import S3_BUCKET, SPARK_CONN_ID, get_default_args

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "tips.csv")

with DAG(
    dag_id="mid_airflow_spark_dag",
    description="[Learning] Tier 2 -- Airflow orchestrating a real Spark job over s3a.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    default_args=get_default_args("learning"),
    catchup=False,
    tags=["learning", "mid", "airflow", "spark"],
) as dag:

    @task
    def seed_and_export() -> str:
        """Stage the real dataset in S3 -- same shape as
        reconciliation_dag.py's seed_and_export, real tips.csv rows instead
        of a hardcoded list.

        Computes its own run key in Python instead of the {{ ds_nodash }}
        Jinja macro reconciliation_dag.py relies on -- Airflow 3 leaves
        logical_date (and everything derived from it: ds, ds_nodash, ts...)
        unset for manually triggered runs of a schedule=None DAG, which
        this DAG is, so {{ ds_nodash }} raises UndefinedError at render
        time on an unscheduled manual trigger. run_id is always set, but
        its manual__<timestamp+tz> shape isn't S3-key-friendly, hence a
        plain compact timestamp computed here instead."""
        with open(DATA_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        run_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        key = f"mid-airflow-spark/{run_key}/tips.csv"
        upload_csv_to_s3(S3_BUCKET, key, rows)
        print(f"Uploaded {len(rows)} rows to s3://{S3_BUCKET}/{key}")
        return run_key

    seed_task = seed_and_export()

    spark_aggregate = SparkSubmitOperator(
        task_id="spark_aggregate",
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/spark_apps/mid_airflow_spark_job.py",
        application_args=[
            "s3a://" + S3_BUCKET + "/mid-airflow-spark/{{ ti.xcom_pull(task_ids='seed_and_export') }}/tips.csv",
            "s3a://" + S3_BUCKET + "/mid-airflow-spark-output/{{ ti.xcom_pull(task_ids='seed_and_export') }}",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.4.1",
        conf={
            "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "spark.jars.ivy": "/opt/airflow/.ivy2",
        },
        # verbose=True (spark-submit's own --verbose flag, separate from
        # Spark's internal log4j level) dumps the entire resolved config and
        # classpath before the job even starts -- off by default, only worth
        # flipping back on when debugging a spark-submit invocation itself.
        verbose=False,
    )

    @task
    def read_summary(run_key: str):
        """Close the loop -- pull the single JSON report Spark wrote and
        print it, so the aggregation is visible in this task's logs
        instead of only in Spark's own UI. Takes seed_and_export's run_key
        directly as a TaskFlow argument (an XCom pull, same as
        ez_hello_dag.py's transform(extract())) rather than re-deriving it
        from context, for the same reason spark_aggregate above avoids
        {{ ds_nodash }}."""
        prefix = f"mid-airflow-spark-output/{run_key}/"
        keys = [k for k in list_s3_objects(S3_BUCKET, prefix=prefix) if "part-" in k]
        if not keys:
            raise FileNotFoundError(f"No Spark output found under s3://{S3_BUCKET}/{prefix}")
        report = json.loads(download_from_s3(S3_BUCKET, keys[0]))
        print(json.dumps(report, indent=2))

    seed_task >> spark_aggregate >> read_summary(seed_task)
